require("dotenv").config();
const express = require("express");
const http = require("http");
const socketIo = require("socket.io");
const axios = require("axios");
const mysql = require("mysql2/promise");
const { OpenAI } = require("openai");
const pLimitModule = require("p-limit");
const cron = require("node-cron");

const pLimit = typeof pLimitModule === "function" ? pLimitModule : pLimitModule.default;

const app = express();
const server = http.createServer(app);
const io = socketIo(server);
const port = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static("public"));

// Lưu trữ client để phát tín hiệu
io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);
  socket.on("disconnect", () => {
    console.log("Client disconnected:", socket.id);
  });
});

let lastApiOrderCount = 0;

// Cấu hình cơ sở dữ liệu và OpenAI
const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
};

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const API_1 = process.env.API_1_URL;
const API_2_BASE = process.env.API_2_BASE_URL;
const TOMTOM_API_KEY = process.env.TOMTOM_API_KEY;
const WAREHOUSE_ADDRESS = process.env.WAREHOUSE_ADDRESS;
const DEFAULT_ADDRESS = {
  DcGiaohang: process.env.DEFAULT_ADDRESS,
  District: "Quận Tân Bình",
  Ward: "Phường 4",
  Source: "Default",
  Distance: 0,
  TravelTime: 0,
};

const TRANSPORT_KEYWORDS = ["XE", "CHÀNH XE", "GỬI XE", "NHÀ XE", "XE KHÁCH"];

// Kiểm tra biến môi trường
if (!process.env.OPENAI_API_KEY) {
  console.error("Lỗi: Thiếu OPENAI_API_KEY trong biến môi trường");
  process.exit(1);
}
if (!process.env.TOMTOM_API_KEY) {
  console.error("Lỗi: Thiếu TOMTOM_API_KEY trong biến môi trường");
  process.exit(1);
}
if (!process.env.API_1_URL) {
  console.error("Lỗi: Thiếu API_1_URL trong biến môi trường");
  process.exit(1);
}
if (!process.env.API_2_BASE_URL) {
  console.error("Lỗi: Thiếu API_2_BASE_URL trong biến môi trường");
  process.exit(1);
}
if (!process.env.WAREHOUSE_ADDRESS) {
  console.error("Lỗi: Thiếu WAREHOUSE_ADDRESS trong biến môi trường");
  process.exit(1);
}

async function retry(fn, retries = 3, minTimeout = 2000, maxTimeout = 10000) {
  let attempt = 0;
  while (attempt < retries) {
    try {
      return await fn();
    } catch (error) {
      attempt++;
      if (attempt >= retries) {
        throw error;
      }
      const delay = Math.min(minTimeout * Math.pow(2, attempt - 1), maxTimeout);
      console.log(`Thử lại sau ${delay}ms (lần ${attempt}/${retries})`);
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

function isTransportAddress(address) {
  if (!address) return false;
  const lowerAddress = address.toUpperCase();
  return TRANSPORT_KEYWORDS.some((keyword) => lowerAddress.includes(keyword));
}

function preprocessAddress(address) {
  if (!address) return "";
  let cleanedAddress = address
    .replace(/\b\d{10,11}\b/g, "")
    .replace(/\([^)]*\)/g, "")
    .replace(/Chiều *: *Giao trước \d{1,2}(g|h)\d{0,2}/gi, "")
    .replace(/[-–/]\s*\w+\s*$/, "")
    .replace(/\s*-\s*/g, "-")
    .replace(/\s+/g, " ")
    .trim();
  return cleanedAddress;
}

function normalizeTransportName(name) {
  if (!name) return "";
  let normalized = name
    .toUpperCase()
    .replace(/^(GỬI\s+)?(?:XE|CHÀNH\s+XE|NHÀ\s+XE|XE\s+KHÁCH)\s+/i, "")
    .replace(/\s*-\s*(D|D1|F[5-8]|A[1-8]|B[1-8]|C[1-8]|G[1-8]|R7|I1)$/i, "")
    .replace(/\s+/g, " ")
    .trim();
  return normalized;
}

function isValidAddress(address) {
  if (!address || address.trim() === "") return false;
  return true;
}

function isInHoChiMinhCity(address) {
  if (!address) return false;
  const lowerAddress = address.toLowerCase();
  return (
    lowerAddress.includes("hồ chí minh") ||
    lowerAddress.includes("tp. hồ chí minh") ||
    !lowerAddress.includes("hà nội")
  );
}

async function getValidOrderIds() {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const [rows] = await connection.execute("SELECT id_order FROM orders");
    await connection.end();
    console.log("Valid order IDs:", rows.map((row) => row.id_order));
    console.log(`getValidOrderIds thực thi trong ${Date.now() - startTime}ms`);
    return new Set(rows.map((row) => row.id_order));
  } catch (error) {
    console.error("Lỗi khi lấy danh sách id_order:", error.message);
    return new Set();
  }
}

async function checkRouteCache(originAddress, destinationAddress) {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const [rows] = await connection.execute(
      `
      SELECT distance, travel_time, calculated_at
      FROM route_cache
      WHERE origin_address = ? 
        AND destination_address = ?
        AND calculated_at >= DATE_SUB(NOW(), INTERVAL 4 HOUR)
        AND calculated_at <= DATE_ADD(NOW(), INTERVAL 4 HOUR)
      ORDER BY calculated_at DESC
      LIMIT 1
      `,
      [originAddress, destinationAddress]
    );
    await connection.end();

    if (rows.length > 0) {
      console.log(
        `Lấy tuyến đường từ cache cho ${originAddress} đến ${destinationAddress} (calculated_at: ${rows[0].calculated_at})`
      );
      console.log(`checkRouteCache thực thi trong ${Date.now() - startTime}ms`);
      return { distance: rows[0].distance, travelTime: rows[0].travel_time };
    }
    console.log(
      `Không tìm thấy cache hợp lệ cho ${originAddress} đến ${destinationAddress} trong khoảng ±2 tiếng`
    );
    return null;
  } catch (error) {
    console.error("Lỗi khi kiểm tra route_cache:", error.message);
    return null;
  }
}

async function saveRouteToCache(
  originAddress,
  destinationAddress,
  distance,
  travelTime
) {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    await connection.execute(
      `
      INSERT INTO route_cache (origin_address, destination_address, distance, travel_time, calculated_at)
      VALUES (?, ?, ?, ?, NOW())
      ON DUPLICATE KEY UPDATE
      distance = VALUES(distance),
      travel_time = VALUES(travel_time),
      calculated_at = NOW()
      `,
      [originAddress, destinationAddress, distance, travelTime]
    );
    await connection.end();
    console.log(
      `Lưu tuyến đường vào cache: ${originAddress} đến ${destinationAddress}`
    );
    console.log(`saveRouteToCache thực thi trong ${Date.now() - startTime}ms`);
  } catch (error) {
    console.error("Lỗi khi lưu route_cache:", error.message);
  }
}

async function geocodeAddress(address) {
  const startTime = Date.now();
  const run = async () => {
    const response = await axios.get(
      `${process.env.TOMTOM_GEOCODE_API_URL}/${encodeURIComponent(
        address
      )}.json`,
      {
        params: {
          key: TOMTOM_API_KEY,
          countrySet: "VN",
          limit: 1,
        },
      }
    );

    if (response.data.results && response.data.results.length > 0) {
      const { lat, lon } = response.data.results[0].position;
      return { lat, lon };
    }
    console.log(`Không tìm thấy tọa độ cho địa chỉ: ${address}`);
    return null;
  };

  try {
    const result = await retry(run);
    console.log(`geocodeAddress thực thi trong ${Date.now() - startTime}ms`);
    return result;
  } catch (error) {
    console.error(
      `Lỗi khi gọi TomTom Geocoding API cho ${address}:`,
      error.message
    );
    return null;
  }
}

async function calculateRoute(originAddress, destinationAddress) {
  const startTime = Date.now();
  if (!isInHoChiMinhCity(destinationAddress)) {
    console.log(
      `Bỏ qua tuyến đường cho ${destinationAddress}: Ngoài TP. Hồ Chí Minh`
    );
    return { distance: null, travelTime: null };
  }

  const cacheResult = await checkRouteCache(originAddress, destinationAddress);
  if (cacheResult) {
    return cacheResult;
  }

  const run = async () => {
    const origin = await geocodeAddress(originAddress);
    const destination = await geocodeAddress(destinationAddress);

    if (!origin || !destination) {
      console.log(
        `Không thể tính tuyến đường từ ${originAddress} đến ${destinationAddress}: Thiếu tọa độ`
      );
      return { distance: null, travelTime: null };
    }

    const response = await axios.get(
      `${process.env.TOMTOM_ROUTING_API_URL}/${origin.lat},${origin.lon}:${destination.lat},${destination.lon}/json`,
      {
        params: {
          key: TOMTOM_API_KEY,
          travelMode: "car",
          traffic: "live",
        },
      }
    );

    if (response.data.routes && response.data.routes.length > 0) {
      const route = response.data.routes[0];
      const distance = route.summary.lengthInMeters / 1000;
      const travelTime = Math.ceil(route.summary.travelTimeInSeconds / 60);
      console.log(
        `Tuyến đường từ ${originAddress} đến ${destinationAddress}: ${distance} km, ${travelTime} phút`
      );
      return { distance, travelTime };
    }

    console.log(
      `Không tìm thấy tuyến đường từ ${originAddress} đến ${destinationAddress}`
    );
    return { distance: null, travelTime: null };
  };

  try {
    const result = await retry(run);
    if (result.distance !== null || result.travelTime !== null) {
      await saveRouteToCache(
        originAddress,
        destinationAddress,
        result.distance,
        result.travelTime
      );
    }
    console.log(`calculateRoute thực thi trong ${Date.now() - startTime}ms`);
    return result;
  } catch (error) {
    console.error(
      `Lỗi khi gọi TomTom Routing API từ ${originAddress} đến ${destinationAddress}:`,
      error.message
    );
    return { distance: null, travelTime: null };
  }
}

async function findTransportCompany(address) {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const cleanedAddress = preprocessAddress(address);
    const normalizedAddress = normalizeTransportName(cleanedAddress);

    console.log("Tìm nhà xe với cleanedAddress:", cleanedAddress);
    console.log("normalizedAddress:", normalizedAddress);

    if (!cleanedAddress) {
      await connection.end();
      console.log("Địa chỉ rỗng sau chuẩn hóa, bỏ qua.");
      return null;
    }

    const [rows] = await connection.execute(
      `
      SELECT standardized_address, district, ward, source
      FROM transport_companies
      WHERE name LIKE ? OR name LIKE ?
      `,
      [`%${cleanedAddress}%`, `%${normalizedAddress}%`]
    );

    await connection.end();

    if (rows.length > 0) {
      console.log("Tìm thấy nhà xe:", rows[0]);
      console.log(
        `findTransportCompany thực thi trong ${Date.now() - startTime}ms`
      );
      return {
        DcGiaohang: rows[0].standardized_address,
        District: rows[0].district,
        Ward: rows[0].ward,
        Source: "TransportDB",
      };
    }
    console.log(`Không tìm thấy nhà xe cho: ${cleanedAddress}`);
    return null;
  } catch (error) {
    console.error("Lỗi khi tìm kiếm nhà xe:", error.message);
    return null;
  }
}

async function fetchAndSaveOrders() {
  const startTime = Date.now();
  try {
    let api2RequestCount = 0;
    const response1 = await axios.get(API_1);
    const orders = response1.data;
    console.log("Số lượng đơn hàng từ API 1:", orders.length);

    if (orders.length === lastApiOrderCount && orders.length > 0) {
      console.log("Số lượng đơn hàng không thay đổi, bỏ qua gọi API_2.");
      console.log("Tổng số yêu cầu API_2:", api2RequestCount);
      return [];
    }

    lastApiOrderCount = orders.length;

    const limit = pLimit(10);
    const api2Promises = orders.map((order) =>
      limit(() => {
        api2RequestCount++;
        return axios
          .get(`${API_2_BASE}?qc=${order.MaPX}`)
          .then((res) => ({
            MaPX: order.MaPX,
            DcGiaohang: res.data.DcGiaohang || "",
            Tinhtranggiao: res.data.Tinhtranggiao || "",
            SOKM: order.SOKM || null, // Lấy SOKM từ API 1
            isEmpty: !res.data.DcGiaohang,
          }))
          .catch((err) => {
            console.error(
              `Lỗi khi gọi API 2 cho MaPX ${order.MaPX}:`,
              err.message
            );
            return null;
          });
      })
    );

    const settledResults = await Promise.allSettled(api2Promises);
    const results = settledResults
      .filter(
        (result) => result.status === "fulfilled" && result.value !== null
      )
      .map((result) => result.value);

    console.log("Tổng số yêu cầu API_2:", api2RequestCount);

    const pendingOrders = results.filter(
      (order) => order.Tinhtranggiao === "Chờ xác nhận giao/lấy hàng"
    );
    console.log("Số lượng đơn hàng chưa giao:", pendingOrders.length);

    if (pendingOrders.length === 0) {
      console.log(
        `fetchAndSaveOrders thực thi trong ${Date.now() - startTime}ms`
      );
      return [];
    }

    const connection = await mysql.createConnection(dbConfig);
    const values = pendingOrders.map((order) => [
      order.MaPX,
      order.DcGiaohang,
      order.Tinhtranggiao,
      order.SOKM,
      new Date(),
    ]);
    const [insertResult] = await connection.query(
      `
      INSERT INTO orders (id_order, address, status, SOKM, created_at)
      VALUES ?
      ON DUPLICATE KEY UPDATE
      address = VALUES(address),
      status = VALUES(status),
      SOKM = VALUES(SOKM),
      created_at = VALUES(created_at)
      `,
      [values]
    );
    console.log(
      "Số dòng ảnh hưởng khi lưu vào cơ sở dữ liệu (orders):",
      insertResult.affectedRows
    );

    const [savedOrders] = await connection.query(
      `
      SELECT id_order
      FROM orders
      WHERE id_order IN (?)
        AND created_at >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)
        AND status = 'Chờ xác nhận giao/lấy hàng'
      `,
      [pendingOrders.map((order) => order.MaPX)]
    );
    const savedMaPX = new Set(savedOrders.map((order) => order.id_order));

    const validResults = pendingOrders.filter((order) =>
      savedMaPX.has(order.MaPX)
    );
    console.log("Số lượng đơn hàng mới và hợp lệ:", validResults.length);
    await connection.end();
    console.log(
      `fetchAndSaveOrders thực thi trong ${Date.now() - startTime}ms`
    );
    return validResults;
  } catch (error) {
    console.error("Lỗi trong fetchAndSaveOrders:", error.message);
    throw error;
  }
}

async function standardizeAddresses(orders) {
  const startTime = Date.now();
  try {
    const standardizedOrders = [];
    let transportResults = [];
    const limit = pLimit(10);

    const openAIPromises = orders.map((order) =>
      limit(async () => {
        const { MaPX, DcGiaohang, isEmpty } = order;

        if (!isValidAddress(DcGiaohang)) {
          console.log(`Bỏ qua địa chỉ không hợp lệ: ${DcGiaohang}`);
          return {
            MaPX,
            DcGiaohang: null,
            District: null,
            Ward: null,
            Source: null,
            isEmpty: true,
          };
        }

        if (DcGiaohang.trim().toUpperCase() === "CHUYỂN PHÁT NHANH") {
          console.log(
            `Gán địa chỉ mặc định cho MaPX ${MaPX}: CHUYỂN PHÁT NHANH`
          );
          return {
            MaPX,
            ...DEFAULT_ADDRESS,
            isEmpty: false,
          };
        }

        const cleanedAddress = preprocessAddress(DcGiaohang);
        if (!cleanedAddress) {
          console.log(`Địa chỉ sau khi làm sạch rỗng: ${DcGiaohang}`);
          return {
            MaPX,
            DcGiaohang: DcGiaohang,
            District: null,
            Ward: null,
            Source: "Original",
            isEmpty: false,
          };
        }

        const prompt = `
        Bạn là một AI chuyên chuẩn hóa địa chỉ tại Việt Nam, có khả năng xử lý địa chỉ ở tất cả các tỉnh/thành phố.

        Yêu cầu cụ thể:
        1. Chuẩn hóa địa chỉ trong trường "DcGiaohang" thành định dạng đầy đủ: "[Số nhà, Đường], [Phường/Xã], [Quận/Huyện/Thị xã/Thành phố], [Tỉnh/Thành phố], Việt Nam".
        2. Tách riêng Quận/Huyện/Thị xã/Thành phố vào trường "District" và Phường/Xã vào trường "Ward".
        3. Loại bỏ thông tin dư thừa như tên người, số điện thoại, thời gian giao hàng, hoặc chú thích không liên quan.
        4. Ưu tiên thông tin địa chỉ cụ thể như số nhà, tên đường, phường, quận, hoặc tỉnh, ngay cả khi có từ khóa nhà xe như "XE", "CHÀNH XE", "GỬI XE".
        5. Nếu thiếu thông tin Phường/Xã, suy luận Phường/Xã phù hợp dựa trên tên đường và quận (nếu có). Nếu không thể suy luận, đặt "Ward" là null nhưng vẫn cố gắng chuẩn hóa các trường khác.
        6. Nếu thiếu Tỉnh/Thành phố, giả định là "TP. Hồ Chí Minh" khi địa chỉ có quận (ví dụ: Q1, Q2) trừ khi có dấu hiệu rõ ràng thuộc tỉnh khác.
        7. Nếu không thể chuẩn hóa đầy đủ (ví dụ: chỉ có tên nhà xe như "Gửi xe Kim Mã" mà không có số nhà, đường, hoặc khu vực), trả về null cho các trường DcGiaohang, District, Ward.
        8. Xử lý các định dạng số nhà không chuẩn (ví dụ: "174-176-178") như một chuỗi số nhà hợp lệ.

        Ví dụ:
        - "XE ANH KHOA 1390 Võ Văn Kiệt (Góc Chu Văn An) 0936845050 (A Duy)" → 
          {
            "MaPX": "X241019078-N",
            "DcGiaohang": "1390 Võ Văn Kiệt, Phường 1, Quận 6, Hồ Chí Minh, Việt Nam",
            "District": "Quận 6",
            "Ward": "Phường 1",
            "Source": "OpenAI"
          }
        - "Gửi xe Kim Mã" → 
          {
            "MaPX": "X2410190xx-N",
            "DcGiaohang": null,
            "District": null,
            "Ward": null,
            "Source": null
          }
        - "12L NGUYỄN THỊ MINH KHAI P.ĐAKAO Q1" → 
          {
            "MaPX": "TEMP_1",
            "DcGiaohang": "12L Nguyễn Thị Minh Khai, Phường Đa Kao, Quận 1, Hồ Chí Minh, Việt Nam",
            "District": "Quận 1",
            "Ward": "Phường Đa Kao",
            "Source": "OpenAI"
          }
        - "174-176-178 Bùi Thị Xuân - Q1" → 
          {
            "MaPX": "TEMP_2",
            "DcGiaohang": "174-178 Bùi Thị Xuân, Phường Phạm Ngũ Lão, Quận 1, Hồ Chí Minh, Việt Nam",
            "District": "Quận 1",
            "Ward": "Phường Phạm Ngũ Lão",
            "Source": "OpenAI"
          }

        Đầu vào:
        \`\`\`json
        [${JSON.stringify({ MaPX, DcGiaohang: cleanedAddress })}]
        \`\`\`

        Đầu ra:
        Trả về đúng một chuỗi JSON duy nhất, định dạng như sau:
        \`\`\`json
        [
          {
            "MaPX": "X2410190xx-N",
            "DcGiaohang": "Địa chỉ đã được chuẩn hóa đầy đủ hoặc null",
            "District": "Quận/Huyện/Thị xã/Thành phố hoặc null",
            "Ward": "Phường/Xã hoặc null",
            "Source": "OpenAI hoặc null"
          }
        ]
        \`\`\`
        `;

        try {
          const completion = await openai.chat.completions.create({
            model: "gpt-4o",
            messages: [{ role: "system", content: prompt }],
          });

          let content = completion.choices[0].message.content;
          content = content.replace(/```json\n?|\n?```/g, "").trim();
          const result = JSON.parse(content);
          console.log(`OpenAI result for MaPX ${MaPX}:`, result[0]);

          if (result[0].DcGiaohang) {
            return {
              ...result[0],
              Source: "OpenAI",
              isEmpty: false,
            };
          } else {
            return {
              MaPX,
              DcGiaohang: DcGiaohang,
              District: null,
              Ward: null,
              Source: "Original",
              isEmpty: false,
            };
          }
        } catch (error) {
          console.error(`Lỗi khi gọi OpenAI cho MaPX ${MaPX}:`, error.message);
          return {
            MaPX,
            DcGiaohang: DcGiaohang,
            District: null,
            Ward: null,
            Source: "Original",
            isEmpty: false,
          };
        }
      })
    );

    const openAIPromisesResults = await Promise.all(openAIPromises);

    const validOrderIds = await getValidOrderIds();
    const validOpenAIResults = openAIPromisesResults.filter((order) =>
      validOrderIds.has(order.MaPX)
    );

    if (validOpenAIResults.length > 0) {
      const connection = await mysql.createConnection(dbConfig);
      const values = validOpenAIResults.map((order) => [
        order.MaPX,
        order.DcGiaohang,
        order.District,
        order.Ward,
        order.Source,
      ]);

      console.log("Lưu kết quả OpenAI vào orders_address:", values);
      const [result] = await connection.query(
        `INSERT INTO orders_address (id_order, address, district, ward, source) VALUES ? 
         ON DUPLICATE KEY UPDATE 
         address = VALUES(address), 
         district = VALUES(district), 
         ward = VALUES(ward), 
         source = VALUES(source)`,
        [values]
      );
      console.log(
        "Số dòng ảnh hưởng khi lưu kết quả OpenAI vào orders_address:",
        result.affectedRows
      );

      const [nullDistrictWardOrders] = await connection.query(
        `
        SELECT id_order, address, source
        FROM orders_address
        WHERE district IS NULL AND ward IS NULL AND address IS NOT NULL
        `
      );
      console.log(
        "Các bản ghi orders_address có district và ward null:",
        nullDistrictWardOrders
      );

      const transportPromises = nullDistrictWardOrders.map((order) =>
        limit(async () => {
          const { id_order, address, source } = order;

          console.log(
            `Tìm nhà xe cho id_order ${id_order} với địa chỉ: ${address} (Source: ${source})`
          );
          const transportResult = await findTransportCompany(address);
          if (transportResult) {
            console.log(
              `Tìm thấy nhà xe cho id_order ${id_order}:`,
              transportResult
            );
            return {
              MaPX: id_order,
              DcGiaohang: transportResult.DcGiaohang,
              District: transportResult.District,
              Ward: transportResult.Ward,
              Source: transportResult.Source,
              isEmpty: false,
            };
          }
          console.log(
            `Không tìm thấy nhà xe trong transport_companies cho id_order ${id_order}`
          );
          return null;
        })
      );

      transportResults = (await Promise.all(transportPromises)).filter(
        (result) => result !== null
      );

      if (transportResults.length > 0) {
        const transportValues = transportResults.map((order) => [
          order.MaPX,
          order.DcGiaohang,
          order.District,
          order.Ward,
          order.Source,
        ]);

        console.log(
          "Lưu kết quả ánh xạ transport_companies vào orders_address:",
          transportValues
        );
        const [transportInsertResult] = await connection.query(
          `INSERT INTO orders_address (id_order, address, district, ward, source) VALUES ? 
           ON DUPLICATE KEY UPDATE 
           address = VALUES(address), 
           district = VALUES(district), 
           ward = VALUES(ward), 
           source = VALUES(source)`,
          [transportValues]
        );
        console.log(
          "Số dòng ảnh hưởng khi lưu kết quả ánh xạ vào orders_address:",
          transportInsertResult.affectedRows
        );

        for (const result of transportResults) {
          const index = standardizedOrders.findIndex(
            (order) => order.MaPX === result.MaPX
          );
          if (index !== -1) {
            standardizedOrders[index] = result;
          } else {
            standardizedOrders.push(result);
          }
        }
      } else {
        console.log("Không có bản ghi nào được ánh xạ từ transport_companies.");
      }

      await connection.end();
    }

    console.log(
      `standardizeAddresses thực thi trong ${Date.now() - startTime}ms`
    );
    return validOpenAIResults.concat(transportResults);
  } catch (error) {
    console.error("Lỗi trong standardizeAddresses:", error.message);
    throw error;
  }
}

async function updatePriorityStatus(io) {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const [result] = await connection.execute(
      `
      UPDATE orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      SET oa.status = 1
      WHERE oa.status = 0
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
        AND oa.created_at <= DATE_SUB(NOW(), INTERVAL 15 MINUTE)
      `
    );
    await connection.end();
    console.log(
      `Đã cập nhật ${result.affectedRows} đơn hàng thành ưu tiên cao (status = 1)`
    );
    console.log(
      `updatePriorityStatus thực thi trong ${Date.now() - startTime}ms`
    );

    // Phát tín hiệu qua Socket.io nếu có bản ghi được cập nhật
    if (result.affectedRows > 0 && io) {
      io.emit("statusUpdated", {
        message: "Đã cập nhật trạng thái đơn hàng",
        updatedCount: result.affectedRows,
      });
      console.log(
        `Đã gửi tín hiệu statusUpdated qua Socket.io: ${result.affectedRows} đơn hàng`
      );
    }
  } catch (error) {
    console.error("Lỗi trong updatePriorityStatus:", error.message);
  }
}

async function calculateDistances() {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const [orderCount] = await connection.query(
      `
      SELECT COUNT(*) as count
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.address IS NOT NULL
        AND oa.distance IS NULL
        AND oa.travel_time IS NULL
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
        AND o.created_at >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)
      `
    );

    if (orderCount[0].count === 0) {
      console.log("Không có đơn hàng mới để tính khoảng cách, bỏ qua.");
      await connection.end();
      console.log(
        `calculateDistances thực thi trong ${Date.now() - startTime}ms`
      );
      return;
    }

    const [orders] = await connection.query(
      `
      SELECT oa.id_order, oa.address
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.address IS NOT NULL
        AND oa.distance IS NULL
        AND oa.travel_time IS NULL
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
        AND o.created_at >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)
      `
    );
    console.log("Các đơn hàng mới để tính khoảng cách:", orders.length);

    const addressMap = {};
    orders.forEach((order) => {
      if (!addressMap[order.address]) {
        addressMap[order.address] = [];
      }
      addressMap[order.address].push(order.id_order);
    });

    const uniqueAddresses = Object.keys(addressMap);
    console.log("Số địa chỉ duy nhất:", uniqueAddresses.length);

    const limit = pLimit(2);
    const routePromises = uniqueAddresses.map((address) =>
      limit(async () => {
        console.log(`Tính tuyến đường cho địa chỉ: ${address}`);
        const route = await calculateRoute(WAREHOUSE_ADDRESS, address);
        return { address, ...route };
      })
    );

    const routeResults = await Promise.all(routePromises);
    console.log("Kết quả tính khoảng cách:", routeResults);

    const updateValues = [];
    routeResults.forEach((result) => {
      const { address, distance, travelTime } = result;
      addressMap[address].forEach((id_order) => {
        updateValues.push([id_order, distance, travelTime]);
      });
    });

    if (updateValues.length > 0) {
      console.log(
        "Cập nhật khoảng cách và thời gian vào orders_address:",
        updateValues
      );
      const [updateResult] = await connection.query(
        `
        INSERT INTO orders_address (id_order, distance, travel_time)
        VALUES ? 
        ON DUPLICATE KEY UPDATE 
        distance = VALUES(distance), 
        travel_time = VALUES(travel_time)
        `,
        [updateValues]
      );
      console.log(
        "Số dòng ảnh hưởng khi cập nhật khoảng cách và thời gian:",
        updateResult.affectedRows
      );
    }

    await connection.end();
    console.log(
      `calculateDistances thực thi trong ${Date.now() - startTime}ms`
    );
  } catch (error) {
    console.error("Lỗi trong calculateDistances:", error.message);
    throw error;
  }
}

async function updateStandardizedAddresses(data) {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);

    const validOrderIds = await getValidOrderIds();
    const validOrders = data.filter((order) => validOrderIds.has(order.MaPX));

    console.log(
      `Số lượng đơn hàng hợp lệ để lưu vào orders_address: ${validOrders.length}`
    );
    console.log("Valid orders:", JSON.stringify(validOrders, null, 2));

    if (validOrders.length > 0) {
      const values = validOrders.map((order) => [
        order.MaPX,
        order.DcGiaohang,
        order.District,
        order.Ward,
        order.Source,
      ]);

      console.log("Values to insert into orders_address:", values);

      const [result] = await connection.query(
        `INSERT INTO orders_address (id_order, address, district, ward, source) VALUES ? 
         ON DUPLICATE KEY UPDATE 
         address = VALUES(address), 
         district = VALUES(district), 
         ward = VALUES(ward), 
         source = VALUES(source)`,
        [values]
      );
      console.log(
        "Số dòng ảnh hưởng khi lưu vào cơ sở dữ liệu (orders_address):",
        result.affectedRows
      );

      const invalidOrders = data.filter(
        (order) => !validOrderIds.has(order.MaPX)
      );
      if (invalidOrders.length > 0) {
        console.warn(
          "Các MaPX không tồn tại trong bảng orders:",
          invalidOrders.map((order) => order.MaPX)
        );
      }
    } else {
      console.warn("Không có đơn hàng hợp lệ để lưu vào orders_address");
    }

    await connection.end();
    console.log(
      `updateStandardizedAddresses thực thi trong ${Date.now() - startTime}ms`
    );
  } catch (error) {
    console.error("Lỗi trong updateStandardizedAddresses:", error.message);
    throw error;
  }
}

async function groupOrders(page = 1) {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const pageSize = 10;
    const offset = (page - 1) * pageSize;

    if (!Number.isInteger(page) || page < 1) {
      throw new Error("Page phải là số nguyên dương");
    }

    const [totalResult] = await connection.execute(
      `
      SELECT COUNT(*) as total
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.address IS NOT NULL 
        AND oa.distance IS NOT NULL 
        AND oa.distance > 0
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
      `
    );
    const totalOrders = totalResult[0].total;
    const totalPages = Math.ceil(totalOrders / pageSize);

    console.log(
      `groupOrders: page=${page}, pageSize=${pageSize}, offset=${offset}`
    );

    const query = `
      SELECT 
        oa.id_order,
        oa.address,
        oa.source,
        oa.distance,
        oa.travel_time,
        oa.status,
        oa.created_at,
        o.SOKM
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.address IS NOT NULL 
        AND oa.distance IS NOT NULL 
        AND oa.distance > 0
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
      ORDER BY 
        oa.status DESC,
        CASE 
          WHEN oa.status = 1 THEN oa.created_at
          ELSE NULL
        END ASC,
        CASE 
          WHEN oa.status = 1 THEN oa.distance
          ELSE NULL
        END ASC,
        CASE 
          WHEN oa.status = 0 THEN oa.distance
          ELSE NULL
        END ASC,
        CASE 
          WHEN oa.status = 0 THEN oa.travel_time
          ELSE NULL
        END ASC
      LIMIT ${parseInt(pageSize)} OFFSET ${parseInt(offset)}
    `;

    const [results] = await connection.execute(query);

    const parsedResults = results.map((row) => ({
      id_order: row.id_order,
      address: row.address,
      source: row.source,
      distance: row.distance,
      travel_time: row.travel_time,
      status: row.status,
      created_at: row.created_at,
      SOKM: row.SOKM,
    }));

    await connection.end();

    const response = {
      totalOrders,
      totalPages,
      currentPage: page,
      lastRun: new Date().toISOString(),
      orders: parsedResults,
    };

    console.log(`Số đơn hàng trang ${page}:`, parsedResults.length);
    console.log(
      `Tổng số đơn hàng: ${totalOrders}, Tổng số trang: ${totalPages}`
    );
    console.log(`groupOrders thực thi trong ${Date.now() - startTime}ms`);
    return response;
  } catch (error) {
    console.error("Lỗi trong groupOrders:", error.message, error.stack);
    throw error;
  }
}

async function syncOrderStatus() {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const [orders] = await connection.query(
      `
      SELECT id_order
      FROM orders
      WHERE status = 'Chờ xác nhận giao/lấy hàng'
        AND created_at >= DATE_SUB(NOW(), INTERVAL 48 HOUR)
      `
    );
    console.log("Số lượng đơn hàng cần đồng bộ trạng thái:", orders.length);

    let api2RequestCount = 0;
    const limit = pLimit(10);
    const statusPromises = orders.map((order) =>
      limit(() => {
        api2RequestCount++;
        return axios
          .get(`${API_2_BASE}?qc=${order.id_order}`)
          .then((res) => ({
            MaPX: order.id_order,
            Tinhtranggiao: res.data.Tinhtranggiao || "",
          }))
          .catch((err) => {
            console.error(
              `Lỗi khi cập nhật trạng thái cho ${order.id_order}:`,
              err.message
            );
            return null;
          });
      })
    );

    const settledResults = await Promise.allSettled(statusPromises);
    const results = settledResults
      .filter(
        (result) => result.status === "fulfilled" && result.value !== null
      )
      .map((result) => result.value);

    console.log(
      "Tổng số yêu cầu API_2 trong syncOrderStatus:",
      api2RequestCount
    );

    if (results.length > 0) {
      const values = results.map((result) => [
        result.Tinhtranggiao,
        result.MaPX,
      ]);
      const [updateResult] = await connection.query(
        `
        UPDATE orders
        SET status = ?
        WHERE id_order = ?
        `,
        values.flat()
      );
      console.log(
        "Số dòng ảnh hưởng khi cập nhật trạng thái:",
        updateResult.affectedRows
      );
    }

    await connection.end();
    console.log(`syncOrderStatus thực thi trong ${Date.now() - startTime}ms`);
  } catch (error) {
    console.error("Lỗi trong syncOrderStatus:", error.message);
    throw error;
  }
}

async function main(page = 1, io) {
  const startTime = Date.now();
  try {
    console.log("Khởi động công cụ giao hàng lúc:", new Date().toISOString());

    console.log("Bước 0: Cập nhật trạng thái ưu tiên đơn hàng...");
    await updatePriorityStatus(io);
    console.log("Đã cập nhật trạng thái ưu tiên");

    console.log("Bước 1: Lấy và lưu đơn hàng...");
    const orders = await fetchAndSaveOrders();
    console.log("Đã lưu đơn hàng:", orders.length);

    if (orders.length === 0) {
      console.log("Không có đơn hàng mới, lấy danh sách đơn hàng hiện có...");
      const groupedOrders = await groupOrders(page);
      console.log("Kết quả đơn hàng:", JSON.stringify(groupedOrders, null, 2));
      console.log("Công cụ giao hàng hoàn tất.");
      console.log(`main thực thi trong ${Date.now() - startTime}ms`);
      return groupedOrders;
    }

    console.log("Bước 2: Chuẩn hóa và ánh xạ địa chỉ...");
    const standardizedOrders = await standardizeAddresses(orders);
    console.log("Đã chuẩn hóa và ánh xạ đơn hàng:", standardizedOrders.length);

    console.log("Bước 3: Cập nhật địa chỉ chuẩn hóa...");
    await updateStandardizedAddresses(standardizedOrders);
    console.log("Đã cập nhật địa chỉ chuẩn hóa");

    console.log("Bước 4: Tính toán khoảng cách và thời gian...");
    await calculateDistances();
    console.log("Đã tính toán khoảng cách và thời gian");

    console.log(`Bước 5: Lấy đơn hàng gần nhất (trang ${page})...`);
    const groupedOrders = await groupOrders(page);
    console.log("Kết quả đơn hàng:", JSON.stringify(groupedOrders, null, 2));

    console.log("Công cụ giao hàng hoàn tất.");
    console.log(`main thực thi trong ${Date.now() - startTime}ms`);
    return groupedOrders;
  } catch (error) {
    console.error("Lỗi trong main:", error.message);
    throw error;
  }
}

// Chạy main ngay khi khởi động server
main(1, io).catch((error) =>
  console.error("Lỗi khi chạy main lần đầu:", error.message)
);

// Lập lịch chạy tự động mỗi 5 phút
cron.schedule("*/5 * * * *", () => {
  console.log("Chạy quy trình giao hàng lúc:", new Date().toISOString());
  main(1, io).catch((error) =>
    console.error("Lỗi khi chạy main:", error.message)
  );
});

// Lập lịch đồng bộ trạng thái mỗi 15 phút
cron.schedule("*/15 * * * *", () => {
  console.log("Chạy đồng bộ trạng thái lúc:", new Date().toISOString());
  syncOrderStatus().catch((error) =>
    console.error("Lỗi khi chạy syncOrderStatus:", error.message)
  );
});

// --------------------------------------------- ROUTE ---------------------------------------------
app.get("/grouped-orders", async (req, res) => {
  try {
    console.time("grouped-orders");
    const page = parseInt(req.query.page) || 1;
    if (isNaN(page) || page < 1) {
      return res.status(400).json({ error: "Page phải là số nguyên dương" });
    }
    console.log(`Gọi groupOrders với page: ${page}`);
    const groupedOrders = await groupOrders(page);
    console.timeEnd("grouped-orders");
    res.status(200).json(groupedOrders);
  } catch (error) {
    console.error("Lỗi trong /grouped-orders:", error.message, error.stack);
    res.status(500).json({ error: "Lỗi server", details: error.message });
  }
});

app.get("/process-orders", async (req, res) => {
  try {
    console.time("process-orders");
    const page = parseInt(req.query.page) || 1;
    if (isNaN(page) || page < 1) {
      return res.status(400).json({ error: "Page phải là số nguyên dương" });
    }
    console.log(`Gọi main với page: ${page}`);
    const groupedOrders = await main(page, io);
    console.timeEnd("process-orders");
    res.status(200).json(groupedOrders);
  } catch (error) {
    console.error("Lỗi trong /process-orders:", error.message, error.stack);
    res.status(500).json({ error: "Lỗi server", details: error.message });
  }
});

server.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});