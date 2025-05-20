require("dotenv").config();
const express = require("express");
const http = require("http");
const socketIo = require("socket.io");
const axios = require("axios");
const mysql = require("mysql2/promise");
const { OpenAI } = require("openai");
const pLimitModule = require("p-limit");
const cron = require("node-cron");
const moment = require("moment-timezone");

const pLimit =
  typeof pLimitModule === "function" ? pLimitModule : pLimitModule.default;

const app = express();
const server = http.createServer(app);
const io = socketIo(server);
const port = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static("public"));

function getNextCronRunTime() {
  const now = moment().tz("Asia/Ho_Chi_Minh");
  const minutes = now.minute();
  const nextMinute = Math.ceil((minutes + 1) / 5) * 5;
  return now
    .startOf("hour")
    .minute(nextMinute)
    .second(0)
    .format("YYYY-MM-DD HH:mm:ss");
}

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);
  socket.emit("cronTimeUpdate", {
    nextRunTime: getNextCronRunTime(),
  });
  socket.on("disconnect", () => {
    console.log("Client disconnected:", socket.id);
  });
});

let lastApiOrderCount = 0;

const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
};

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const API_1 = process.env.API_1_URL;
const API_2_BASE = process.env.API_2_BASE_URL;
const SLUG = process.env.SLUG;
const TOMTOM_API_KEY = process.env.TOMTOM_API_KEY;
const WAREHOUSE_ADDRESS = process.env.WAREHOUSE_ADDRESS;

const TRANSPORT_KEYWORDS = ["XE", "CHÀNH XE", "GỬI XE", "NHÀ XE", "XE KHÁCH"];

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

async function getValidOrderIds() {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const [rows] = await connection.execute("SELECT id_order FROM orders");
    await connection.end();
    console.log(
      "Valid order IDs:",
      rows.map((row) => row.id_order)
    );
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
        AND calculated_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
        AND calculated_at <= DATE_ADD(NOW(), INTERVAL 1 HOUR)
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
      const distance = route.summary.lengthInMeters / 1000; // km
      const travelTime = Math.ceil(route.summary.travelTimeInSeconds / 60); // phút
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
    if (result.distance !== null && result.travelTime !== null) {
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

    const connection = await mysql.createConnection(dbConfig);
    const [existingOrders] = await connection.query(
      `SELECT id_order, address, old_address FROM orders WHERE id_order IN (?)`,
      [orders.map((order) => order.MaPX)]
    );
    const addressMap = new Map(
      existingOrders.map((o) => [
        o.id_order,
        { address: o.address, old_address: o.old_address },
      ])
    );

    const limit = pLimit(5);
    const api2Promises = orders.map((order) =>
      limit(() => {
        api2RequestCount++;
        return axios
          .get(`${API_2_BASE}?qc=${order.MaPX}`)
          .then((res) => {
            const currentAddress = addressMap.get(order.MaPX)?.address || "";
            const newAddress = res.data.DcGiaohang || "";
            const addressChanged =
              currentAddress && currentAddress !== newAddress;
            return {
              MaPX: order.MaPX,
              DcGiaohang: newAddress,
              Tinhtranggiao: res.data.Tinhtranggiao || "",
              SOKM: order.SOKM || null,
              Ghichu: order.GhiChu || null,
              Ngayxuatkho: order.Ngayxuatkho || null,
              NgayPX: order.NgayPX || null,
              isEmpty: !newAddress,
              addressChanged,
              old_address: addressChanged
                ? currentAddress
                : addressMap.get(order.MaPX)?.old_address || null,
            };
          })
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
      await connection.end();
      console.log(
        `fetchAndSaveOrders thực thi trong ${Date.now() - startTime}ms`
      );
      return [];
    }

    const values = pendingOrders.map((order) => {
      const ngayPX = order.NgayPX
        ? moment(order.NgayPX, "DD/MM/YYYY HH:mm:ss")
            .tz("Asia/Ho_Chi_Minh")
            .format("YYYY-MM-DD HH:mm:ss")
        : moment().tz("Asia/Ho_Chi_Minh").format("YYYY-MM-DD HH:mm:ss");
      return [
        order.MaPX,
        order.DcGiaohang,
        order.Tinhtranggiao,
        order.SOKM,
        order.Ghichu,
        order.Ngayxuatkho,
        ngayPX,
        order.old_address,
      ];
    });

    const [insertResult] = await connection.query(
      `
      INSERT INTO orders (id_order, address, status, SOKM, delivery_note, date_delivery, created_at, old_address)
      VALUES ?
      ON DUPLICATE KEY UPDATE
      address = VALUES(address),
      status = VALUES(status),
      SOKM = VALUES(SOKM),
      delivery_note = VALUES(delivery_note),
      date_delivery = VALUES(date_delivery),
      created_at = VALUES(created_at),
      old_address = IF(VALUES(old_address) IS NOT NULL AND old_address IS NULL, VALUES(old_address), old_address)
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

    const connection = await mysql.createConnection(dbConfig);
    const [existingAddresses] = await connection.query(
      `
      SELECT id_order, address, district, ward, source
      FROM orders_address
      WHERE id_order IN (?)
        AND address IS NOT NULL
        AND district IS NOT NULL
        AND ward IS NOT NULL
      `,
      [orders.map((order) => order.MaPX)]
    );
    await connection.end();

    const addressMap = new Map(
      existingAddresses.map((row) => [
        row.id_order,
        {
          address: row.address,
          district: row.district,
          ward: row.ward,
          source: row.source,
        },
      ])
    );

    const openAIPromises = orders.map((order) =>
      limit(async () => {
        const { MaPX, DcGiaohang, isEmpty } = order;

        const existingAddress = addressMap.get(MaPX);
        if (existingAddress) {
          console.log(
            `Bỏ qua gọi OpenAI cho MaPX ${MaPX}: Đã có địa chỉ chuẩn hóa`
          );
          return {
            MaPX,
            DcGiaohang: existingAddress.address,
            District: existingAddress.district,
            Ward: existingAddress.ward,
            Source: existingAddress.source,
            isEmpty: false,
          };
        }

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
            DcGiaohang: process.env.DEFAULT_ADDRESS,
            District: null,
            Ward: null,
            Source: "Default",
            isEmpty: false,
            Distance: null,
            TravelTime: null,
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

        if (isTransportAddress(cleanedAddress)) {
          const transportResult = await findTransportCompany(cleanedAddress);
          if (transportResult) {
            return {
              MaPX,
              DcGiaohang: transportResult.DcGiaohang,
              District: transportResult.District,
              Ward: transportResult.Ward,
              Source: "TransportDB",
              isEmpty: false,
            };
          }
        }

        const prompt = `
        Bạn là một AI chuyên chuẩn hóa địa chỉ tại Việt Nam, có khả năng xử lý địa chỉ ở tất cả các tỉnh/thành phố.

Yêu cầu cụ thể:
1. Chuẩn hóa địa chỉ trong trường "DcGiaohang" thành định dạng đầy đủ: "[Số nhà, Đường], [Phường/Xã], [Quận/Huyện/Thị xã/Thành phố], [Tỉnh/Thành phố], Việt Nam".
2. Tách riêng Quận/Huyện/Thị xã/Thành phố vào trường "District" và Phường/Xã vào trường "Ward".
3. Loại bỏ thông tin dư thừa như tên người, số điện thoại, thời gian giao hàng, hoặc chú thích không liên quan.
4. Ưu tiên thông tin địa chỉ cụ thể như số nhà, tên đường, phường, quận, hoặc tỉnh, ngay cả khi có từ khóa nhà xe như "XE", "CHÀNH XE", "GỬI XE".
5. Kiểm tra tính hợp lệ của phường/xã: Nếu thông tin phường/xã được cung cấp nhưng không hợp lệ (ví dụ, phường không tồn tại trên đường hoặc trong quận/huyện được chỉ định), sửa phường/xã thành phường/xã hợp lệ dựa trên tên đường và quận/huyện. Ví dụ: "191 Bùi Thị Xuân, Phường 6, Quận Tân Bình" là sai vì Phường 6 không tồn tại trên đường Bùi Thị Xuân ở Quận Tân Bình, sửa thành "Phường 1" vì đó là phường hợp lệ.
6. Nếu thiếu thông tin phường/xã, suy luận phường/xã phù hợp dựa trên tên đường và quận/huyện (nếu có). Ví dụ: "191 Bùi Thị Xuân, Quận Tân Bình" nên suy luận thành "Phường 1" vì đường Bùi Thị Xuân thuộc Phường 1, Quận Tân Bình. Nếu không thể suy luận, đặt "Ward" là null nhưng vẫn cố gắng chuẩn hóa các trường khác.
7. Nếu thiếu tỉnh/thành phố, suy luận tỉnh/thành phố dựa trên quận/huyện, tên đường, hoặc từ khóa trong địa chỉ (ví dụ: "Q1" gợi ý TP. Hồ Chí Minh, "Kim Mã" gợi ý Hà Nội). Nếu không thể suy luận, đặt tỉnh/thành phố là null.
8. Nếu không thể chuẩn hóa đầy đủ (ví dụ: chỉ có tên nhà xe như "Gửi xe Kim Mã" mà không có số nhà, đường, hoặc khu vực), trả về null cho các trường DcGiaohang, District, Ward.
9. Xử lý các định dạng số nhà không chuẩn (ví dụ: "174-176-178") như một chuỗi số nhà hợp lệ.
10. Ưu tiên độ chính xác: Khi suy luận hoặc sửa phường/xã, sử dụng thông tin địa lý chính xác của Việt Nam, đặc biệt tại TP. Hồ Chí Minh, nơi các đường phố thường thuộc một phường cụ thể trong quận.

Ví dụ:
- "191 BÙI THỊ XUÂN, PHƯỜNG 6, QUẬN TÂN BÌNH" → 
  {
    "MaPX": "X241019078-N",
    "DcGiaohang": "191 Bùi Thị Xuân, Phường 1, Quận Tân Bình, Hồ Chí Minh, Việt Nam",
    "District": "Quận Tân Bình",
    "Ward": "Phường 1",
    "Source": "OpenAI"
  }
- "191 Bùi Thị Xuân, Quận Tân Bình" → 
  {
    "MaPX": "X241019079-N",
    "DcGiaohang": "191 Bùi Thị Xuân, Phường 1, Quận Tân Bình, Hồ Chí Minh, Việt Nam",
    "District": "Quận Tân Bình",
    "Ward": "Phường 1",
    "Source": "OpenAI"
  }
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
- "123 Trần Hưng Đạo, TP Đà Nẵng" → 
  {
    "MaPX": "TEMP_3",
    "DcGiaohang": "123 Trần Hưng Đạo, Phường Hải Châu I, Quận Hải Châu, Đà Nẵng, Việt Nam",
    "District": "Quận Hải Châu",
    "Ward": "Phường Hải Châu I",
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
            model: "gpt-4o-mini-2024-07-18",
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

    // Tiếp tục xử lý như code gốc
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
        AND o.date_delivery IS NOT NULL
        AND STR_TO_DATE(o.date_delivery, '%d/%m/%Y %H:%i:%s') <= DATE_SUB(NOW(), INTERVAL 15 MINUTE)
      `
    );
    await connection.end();
    console.log(
      `Đã cập nhật ${result.affectedRows} đơn hàng thành ưu tiên cao (status = 1)`
    );
    console.log(
      `updatePriorityStatus thực thi trong ${Date.now() - startTime}ms`
    );
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
        AND (oa.distance IS NULL OR oa.travel_time IS NULL)
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
      `
    );

    if (orderCount[0].count === 0) {
      console.log(
        "Không có đơn hàng mới hoặc cần tính lại khoảng cách, bỏ qua."
      );
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
        AND (oa.distance IS NULL OR oa.travel_time IS NULL)
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
      `
    );
    console.log("Các đơn hàng để tính khoảng cách:", orders.length);

    const addressMap = {};
    const expressDeliveryOrders = [];

    orders.forEach((order) => {
      if (order.address.toUpperCase() === "CHUYỂN PHÁT NHANH") {
        expressDeliveryOrders.push(order.id_order);
      } else {
        if (!addressMap[order.address]) {
          addressMap[order.address] = [];
        }
        addressMap[order.address].push(order.id_order);
      }
    });

    const uniqueAddresses = Object.keys(addressMap);

    const limit = pLimit(2);
    const routePromises = uniqueAddresses.map((address) =>
      limit(async () => {
        console.log(`Tính tuyến đường cho địa chỉ: ${address}`);
        const route = await calculateRoute(WAREHOUSE_ADDRESS, address);
        if (route.distance === null || route.travelTime === null) {
          console.warn(
            `Lỗi tính tuyến đường cho địa chỉ ${address}, gán distance = 0, travel_time = 0`
          );
          return { address, distance: 0, travelTime: 0 };
        }
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

    expressDeliveryOrders.forEach((id_order) => {
      updateValues.push([id_order, null, null]);
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

    if (validOrders.length > 0) {
      const [existingAddresses] = await connection.query(
        `SELECT id_order, distance, travel_time FROM orders_address WHERE id_order IN (?)`,
        [validOrders.map((order) => order.MaPX)]
      );
      const addressMap = new Map(
        existingAddresses.map((o) => [
          o.id_order,
          { distance: o.distance, travel_time: o.travel_time },
        ])
      );

      const values = validOrders.map((order) => {
        const current = addressMap.get(order.MaPX) || {
          distance: null,
          travel_time: null,
        };
        return [
          order.MaPX,
          order.DcGiaohang,
          order.District,
          order.Ward,
          order.Source,
          order.addressChanged ? null : undefined,
          order.addressChanged ? null : undefined,
          order.addressChanged ? current.distance : null,
          order.addressChanged ? current.travel_time : null,
        ];
      });

      console.log("Values to insert into orders_address:", values);

      const [result] = await connection.query(
        `
        INSERT INTO orders_address (
          id_order, address, district, ward, source, 
          distance, travel_time, old_distance, old_travel_time
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
        address = VALUES(address),
        district = VALUES(district),
        ward = VALUES(ward),
        source = VALUES(source),
        distance = IF(VALUES(distance) IS NULL, NULL, distance),
        travel_time = IF(VALUES(travel_time) IS NULL, NULL, travel_time),
        old_distance = IF(VALUES(old_distance) IS NOT NULL, VALUES(old_distance), old_distance),
        old_travel_time = IF(VALUES(old_travel_time) IS NOT NULL, VALUES(old_travel_time), old_travel_time)
        `,
        values.flat()
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

async function groupOrders(page = 1, filterDate = null) {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const pageSize = 10;

    if (!Number.isInteger(page) || page < 1) {
      throw new Error("Page phải là số nguyên dương");
    }

    let dateCondition = "";
    let queryParams = [];

    if (filterDate) {
      if (!moment(filterDate, "YYYY-MM-DD", true).isValid()) {
        throw new Error("Định dạng ngày không hợp lệ, sử dụng YYYY-MM-DD");
      }
      dateCondition =
        "DATE(STR_TO_DATE(o.date_delivery, '%d/%m/%Y %H:%i:%s')) = ?";
      queryParams.push(filterDate);
    }

    const whereClause = dateCondition ? `WHERE ${dateCondition}` : "";

    // Lấy tổng số đơn hàng
    const [totalResult] = await connection.execute(
      `
      SELECT COUNT(*) as total
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.address IS NOT NULL 
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
        ${dateCondition}
      `,
      queryParams
    );

    const totalOrders = totalResult[0].total;
    const totalPages = Math.ceil(totalOrders / pageSize);

    // Lấy toàn bộ dữ liệu mà không áp dụng LIMIT và OFFSET
    const query = `
      SELECT 
        oa.id_order,
        oa.address,
        oa.source,
        oa.distance,
        oa.travel_time,
        oa.status,
        oa.created_at,
        oa.district,
        oa.ward,
        oa.old_distance,
        oa.old_travel_time,
        o.SOKM,
        o.priority,
        o.delivery_deadline,
        o.date_delivery,
        o.delivery_note,
        o.address AS current_address,
        o.old_address,
        CASE 
          WHEN DATE(STR_TO_DATE(o.date_delivery, '%d/%m/%Y %H:%i:%s')) <= CURDATE() - INTERVAL 2 DAY THEN 2
          WHEN DATE(STR_TO_DATE(o.date_delivery, '%d/%m/%Y %H:%i:%s')) = CURDATE() - INTERVAL 1 DAY THEN 1 
          ELSE 0 
        END AS days_old,
        TIMESTAMPDIFF(MINUTE, oa.created_at, NOW()) AS minutes_since_created
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.address IS NOT NULL 
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
        ${dateCondition}
    `;

    const [results] = await connection.execute(query, queryParams);
    await connection.end();

    // Chuyển đổi kết quả thành định dạng mong muốn
    const parsedResults = results.map((row) => ({
      id_order: row.id_order,
      address: row.address || "N/A",
      source: row.source,
      distance:
        row.distance !== null ? parseFloat(row.distance.toFixed(2)) : null,
      travel_time: row.travel_time !== null ? row.travel_time : null,
      status: row.status,
      created_at: row.created_at
        ? moment(row.created_at)
            .tz("Asia/Ho_Chi_Minh")
            .format("YYYY-MM-DD HH:mm:ss")
        : null,
      district: row.district || "N/A",
      ward: row.ward || "N/A",
      old_distance:
        row.old_distance !== null
          ? parseFloat(row.old_distance.toFixed(2))
          : null,
      old_travel_time:
        row.old_travel_time !== null ? row.old_travel_time : null,
      SOKM:
        row.SOKM !== null && !isNaN(parseFloat(row.SOKM))
          ? parseFloat(parseFloat(row.SOKM).toFixed(2))
          : null,
      priority: row.priority,
      delivery_deadline: row.delivery_deadline
        ? moment(row.delivery_deadline)
            .tz("Asia/Ho_Chi_Minh")
            .format("YYYY-MM-DD HH:mm:ss")
        : null,
      date_delivery: row.date_delivery,
      delivery_note: row.delivery_note,
      current_address: row.current_address,
      old_address: row.old_address,
      days_old: row.days_old,
      minutes_since_created:
        row.minutes_since_created !== null ? row.minutes_since_created : 0,
    }));

    // Sắp xếp toàn bộ dữ liệu trong JavaScript
    const sortedResults = parsedResults.sort((a, b) => {
      // Tiêu chí 1: Điểm ưu tiên chính (Priority Score)
      let priorityA, priorityB;

      if (
        !a.district ||
        !a.ward ||
        a.distance === null ||
        a.travel_time === null
      ) {
        priorityA = 100;
      } else if (a.priority === 2) {
        priorityA = 0;
      } else if (
        a.status === 1 &&
        a.priority === 1 &&
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityA = 1;
      } else if (
        a.days_old === 2 &&
        a.status === 1 &&
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityA = 2;
      } else if (
        a.days_old === 2 &&
        a.status === 0 &&
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityA = 3;
      } else if (
        a.days_old === 1 &&
        a.status === 1 &&
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityA = 4;
      } else if (
        a.days_old === 1 &&
        a.status === 0 &&
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityA = 5;
      } else if (a.status === 1 && a.priority === 0) {
        priorityA = 10;
      } else if (
        a.status === 1 &&
        a.priority === 1 &&
        (!a.delivery_deadline ||
          moment(a.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityA = 11;
      } else if (
        a.status === 0 &&
        a.priority === 1 &&
        (!a.delivery_deadline ||
          moment(a.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityA = 12;
      } else if (a.status === 0 && a.priority === 0) {
        priorityA = 13;
      } else if (
        a.days_old === 2 &&
        (!a.delivery_deadline ||
          moment(a.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityA = 14;
      } else if (
        a.days_old === 1 &&
        (!a.delivery_deadline ||
          moment(a.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityA = 15;
      } else {
        priorityA = 16;
      }

      if (
        !b.district ||
        !b.ward ||
        b.distance === null ||
        b.travel_time === null
      ) {
        priorityB = 100;
      } else if (b.priority === 2) {
        priorityB = 0;
      } else if (
        b.status === 1 &&
        b.priority === 1 &&
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityB = 1;
      } else if (
        b.days_old === 2 &&
        b.status === 1 &&
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityB = 2;
      } else if (
        b.days_old === 2 &&
        b.status === 0 &&
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityB = 3;
      } else if (
        b.days_old === 1 &&
        b.status === 1 &&
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityB = 4;
      } else if (
        b.days_old === 1 &&
        b.status === 0 &&
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityB = 5;
      } else if (b.status === 1 && b.priority === 0) {
        priorityB = 10;
      } else if (
        b.status === 1 &&
        b.priority === 1 &&
        (!b.delivery_deadline ||
          moment(b.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityB = 11;
      } else if (
        b.status === 0 &&
        b.priority === 1 &&
        (!b.delivery_deadline ||
          moment(b.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityB = 12;
      } else if (b.status === 0 && b.priority === 0) {
        priorityB = 13;
      } else if (
        b.days_old === 2 &&
        (!b.delivery_deadline ||
          moment(b.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityB = 14;
      } else if (
        b.days_old === 1 &&
        (!b.delivery_deadline ||
          moment(b.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityB = 15;
      } else {
        priorityB = 16;
      }

      if (priorityA !== priorityB) {
        return priorityA - priorityB;
      }

      // Tiêu chí 2: Delivery Deadline trong ngày hiện tại
      const isDeadlineTodayA =
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSame(moment(), "day")
          ? 0
          : 1;
      const isDeadlineTodayB =
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSame(moment(), "day")
          ? 0
          : 1;
      if (isDeadlineTodayA !== isDeadlineTodayB) {
        return isDeadlineTodayA - isDeadlineTodayB;
      }

      // Tiêu chí 3: Khoảng thời gian đến Delivery Deadline
      const timeToDeadlineA = a.delivery_deadline
        ? moment(a.delivery_deadline).diff(moment(), "minutes")
        : 999999;
      const timeToDeadlineB = b.delivery_deadline
        ? moment(b.delivery_deadline).diff(moment(), "minutes")
        : 999999;
      if (timeToDeadlineA !== timeToDeadlineB) {
        return timeToDeadlineA - timeToDeadlineB;
      }

      // Tiêu chí 4: Distance
      const distanceA = a.distance !== null ? a.distance : 999999;
      const distanceB = b.distance !== null ? b.distance : 999999;
      if (distanceA !== distanceB) {
        return distanceA - distanceB;
      }

      // Tiêu chí 5: Travel Time
      const travelTimeA = a.travel_time !== null ? a.travel_time : 999999;
      const travelTimeB = b.travel_time !== null ? b.travel_time : 999999;
      if (travelTimeA !== travelTimeB) {
        return travelTimeA - travelTimeB;
      }

      // Tiêu chí 6: Date Delivery
      const dateDeliveryA = a.date_delivery
        ? moment(a.date_delivery, "DD/MM/YYYY HH:mm:ss").isValid()
          ? moment(a.date_delivery, "DD/MM/YYYY HH:mm:ss")
          : moment("9999-12-31 23:59:59")
        : moment("9999-12-31 23:59:59");
      const dateDeliveryB = b.date_delivery
        ? moment(b.date_delivery, "DD/MM/YYYY HH:mm:ss").isValid()
          ? moment(b.date_delivery, "DD/MM/YYYY HH:mm:ss")
          : moment("9999-12-31 23:59:59")
        : moment("9999-12-31 23:59:59");
      if (!dateDeliveryA.isSame(dateDeliveryB)) {
        return dateDeliveryA.diff(dateDeliveryB);
      }

      // Tiêu chí 7: id_order
      return a.id_order.localeCompare(b.id_order);
    });

    // Áp dụng phân trang sau khi đã sắp xếp toàn bộ dữ liệu
    const startIndex = (page - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    const paginatedResults = sortedResults.slice(startIndex, endIndex);

    return {
      totalOrders,
      totalPages,
      currentPage: page,
      lastRun: moment().tz("Asia/Ho_Chi_Minh").format(),
      orders: paginatedResults,
    };
  } catch (error) {
    console.error("Lỗi trong groupOrders:", error.message, error.stack);
    throw error;
  }
}

async function groupOrders2(page = 1, filterDate = null) {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const pageSize = 20;

    if (!Number.isInteger(page) || page < 1) {
      throw new Error("Page phải là số nguyên dương");
    }

    let dateCondition = "";
    let queryParams = [];

    if (filterDate) {
      if (!moment(filterDate, "YYYY-MM-DD", true).isValid()) {
        throw new Error("Định dạng ngày không hợp lệ, sử dụng YYYY-MM-DD");
      }
      dateCondition =
        "DATE(STR_TO_DATE(o.date_delivery, '%d/%m/%Y %H:%i:%s')) = ?";
      queryParams.push(filterDate);
    }

    const whereClause = dateCondition ? `WHERE ${dateCondition}` : "";

    // Lấy tổng số đơn hàng
    const [totalResult] = await connection.execute(
      `
      SELECT COUNT(*) as total
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.address IS NOT NULL 
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
        ${dateCondition}
      `,
      queryParams
    );

    const totalOrders = totalResult[0].total;
    const totalPages = Math.ceil(totalOrders / pageSize);

    // Lấy toàn bộ dữ liệu mà không áp dụng LIMIT và OFFSET
    const query = `
      SELECT 
        oa.id_order,
        oa.address,
        oa.source,
        oa.distance,
        oa.travel_time,
        oa.status,
        oa.created_at,
        oa.district,
        oa.ward,
        oa.old_distance,
        oa.old_travel_time,
        o.SOKM,
        o.priority,
        o.delivery_deadline,
        o.date_delivery,
        o.delivery_note,
        o.address AS current_address,
        o.old_address,
        CASE 
          WHEN DATE(STR_TO_DATE(o.date_delivery, '%d/%m/%Y %H:%i:%s')) <= CURDATE() - INTERVAL 2 DAY THEN 2
          WHEN DATE(STR_TO_DATE(o.date_delivery, '%d/%m/%Y %H:%i:%s')) = CURDATE() - INTERVAL 1 DAY THEN 1 
          ELSE 0 
        END AS days_old,
        TIMESTAMPDIFF(MINUTE, oa.created_at, NOW()) AS minutes_since_created
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.address IS NOT NULL 
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
        ${dateCondition}
    `;

    const [results] = await connection.execute(query, queryParams);
    await connection.end();

    // Chuyển đổi kết quả thành định dạng mong muốn
    const parsedResults = results.map((row) => ({
      id_order: row.id_order,
      address: row.address || "N/A",
      source: row.source,
      distance:
        row.distance !== null ? parseFloat(row.distance.toFixed(2)) : null,
      travel_time: row.travel_time !== null ? row.travel_time : null,
      status: row.status,
      created_at: row.created_at
        ? moment(row.created_at)
            .tz("Asia/Ho_Chi_Minh")
            .format("YYYY-MM-DD HH:mm:ss")
        : null,
      district: row.district || "N/A",
      ward: row.ward || "N/A",
      old_distance:
        row.old_distance !== null
          ? parseFloat(row.old_distance.toFixed(2))
          : null,
      old_travel_time:
        row.old_travel_time !== null ? row.old_travel_time : null,
      SOKM:
        row.SOKM !== null && !isNaN(parseFloat(row.SOKM))
          ? parseFloat(parseFloat(row.SOKM).toFixed(2))
          : null,
      priority: row.priority,
      delivery_deadline: row.delivery_deadline
        ? moment(row.delivery_deadline)
            .tz("Asia/Ho_Chi_Minh")
            .format("YYYY-MM-DD HH:mm:ss")
        : null,
      date_delivery: row.date_delivery,
      delivery_note: row.delivery_note,
      current_address: row.current_address,
      old_address: row.old_address,
      days_old: row.days_old,
      minutes_since_created:
        row.minutes_since_created !== null ? row.minutes_since_created : 0,
    }));

    // Sắp xếp toàn bộ dữ liệu trong JavaScript
    const sortedResults = parsedResults.sort((a, b) => {
      // Tiêu chí 1: Điểm ưu tiên chính (Priority Score)
      let priorityA, priorityB;

      if (
        !a.district ||
        !a.ward ||
        a.distance === null ||
        a.travel_time === null
      ) {
        priorityA = 100;
      } else if (a.priority === 2) {
        priorityA = 0;
      } else if (
        a.status === 1 &&
        a.priority === 1 &&
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityA = 1;
      } else if (
        a.days_old === 2 &&
        a.status === 1 &&
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityA = 2;
      } else if (
        a.days_old === 2 &&
        a.status === 0 &&
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityA = 3;
      } else if (
        a.days_old === 1 &&
        a.status === 1 &&
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityA = 4;
      } else if (
        a.days_old === 1 &&
        a.status === 0 &&
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityA = 5;
      } else if (a.status === 1 && a.priority === 0) {
        priorityA = 10;
      } else if (
        a.status === 1 &&
        a.priority === 1 &&
        (!a.delivery_deadline ||
          moment(a.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityA = 11;
      } else if (
        a.status === 0 &&
        a.priority === 1 &&
        (!a.delivery_deadline ||
          moment(a.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityA = 12;
      } else if (a.status === 0 && a.priority === 0) {
        priorityA = 13;
      } else if (
        a.days_old === 2 &&
        (!a.delivery_deadline ||
          moment(a.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityA = 14;
      } else if (
        a.days_old === 1 &&
        (!a.delivery_deadline ||
          moment(a.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityA = 15;
      } else {
        priorityA = 16;
      }

      if (
        !b.district ||
        !b.ward ||
        b.distance === null ||
        b.travel_time === null
      ) {
        priorityB = 100;
      } else if (b.priority === 2) {
        priorityB = 0;
      } else if (
        b.status === 1 &&
        b.priority === 1 &&
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityB = 1;
      } else if (
        b.days_old === 2 &&
        b.status === 1 &&
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityB = 2;
      } else if (
        b.days_old === 2 &&
        b.status === 0 &&
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityB = 3;
      } else if (
        b.days_old === 1 &&
        b.status === 1 &&
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityB = 4;
      } else if (
        b.days_old === 1 &&
        b.status === 0 &&
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSameOrBefore(moment().add(2, "hours"))
      ) {
        priorityB = 5;
      } else if (b.status === 1 && b.priority === 0) {
        priorityB = 10;
      } else if (
        b.status === 1 &&
        b.priority === 1 &&
        (!b.delivery_deadline ||
          moment(b.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityB = 11;
      } else if (
        b.status === 0 &&
        b.priority === 1 &&
        (!b.delivery_deadline ||
          moment(b.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityB = 12;
      } else if (b.status === 0 && b.priority === 0) {
        priorityB = 13;
      } else if (
        b.days_old === 2 &&
        (!b.delivery_deadline ||
          moment(b.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityB = 14;
      } else if (
        b.days_old === 1 &&
        (!b.delivery_deadline ||
          moment(b.delivery_deadline).isAfter(moment().add(2, "hours")))
      ) {
        priorityB = 15;
      } else {
        priorityB = 16;
      }

      if (priorityA !== priorityB) {
        return priorityA - priorityB;
      }

      // Tiêu chí 2: Delivery Deadline trong ngày hiện tại
      const isDeadlineTodayA =
        a.delivery_deadline &&
        moment(a.delivery_deadline).isSame(moment(), "day")
          ? 0
          : 1;
      const isDeadlineTodayB =
        b.delivery_deadline &&
        moment(b.delivery_deadline).isSame(moment(), "day")
          ? 0
          : 1;
      if (isDeadlineTodayA !== isDeadlineTodayB) {
        return isDeadlineTodayA - isDeadlineTodayB;
      }

      // Tiêu chí 3: Khoảng thời gian đến Delivery Deadline
      const timeToDeadlineA = a.delivery_deadline
        ? moment(a.delivery_deadline).diff(moment(), "minutes")
        : 999999;
      const timeToDeadlineB = b.delivery_deadline
        ? moment(b.delivery_deadline).diff(moment(), "minutes")
        : 999999;
      if (timeToDeadlineA !== timeToDeadlineB) {
        return timeToDeadlineA - timeToDeadlineB;
      }

      // Tiêu chí 4: Distance
      const distanceA = a.distance !== null ? a.distance : 999999;
      const distanceB = b.distance !== null ? b.distance : 999999;
      if (distanceA !== distanceB) {
        return distanceA - distanceB;
      }

      // Tiêu chí 5: Travel Time
      const travelTimeA = a.travel_time !== null ? a.travel_time : 999999;
      const travelTimeB = b.travel_time !== null ? b.travel_time : 999999;
      if (travelTimeA !== travelTimeB) {
        return travelTimeA - travelTimeB;
      }

      // Tiêu chí 6: Date Delivery
      const dateDeliveryA = a.date_delivery
        ? moment(a.date_delivery, "DD/MM/YYYY HH:mm:ss").isValid()
          ? moment(a.date_delivery, "DD/MM/YYYY HH:mm:ss")
          : moment("9999-12-31 23:59:59")
        : moment("9999-12-31 23:59:59");
      const dateDeliveryB = b.date_delivery
        ? moment(b.date_delivery, "DD/MM/YYYY HH:mm:ss").isValid()
          ? moment(b.date_delivery, "DD/MM/YYYY HH:mm:ss")
          : moment("9999-12-31 23:59:59")
        : moment("9999-12-31 23:59:59");
      if (!dateDeliveryA.isSame(dateDeliveryB)) {
        return dateDeliveryA.diff(dateDeliveryB);
      }

      // Tiêu chí 7: id_order
      return a.id_order.localeCompare(b.id_order);
    });

    // Áp dụng phân trang sau khi đã sắp xếp toàn bộ dữ liệu
    const startIndex = (page - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    const paginatedResults = sortedResults.slice(startIndex, endIndex);

    return {
      totalOrders,
      totalPages,
      currentPage: page,
      lastRun: moment().tz("Asia/Ho_Chi_Minh").format(),
      orders: paginatedResults,
    };
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
        AND date_delivery IS NOT NULL
      `
    );
    console.log("Số lượng đơn hàng cần đồng bộ trạng thái:", orders.length);

    if (orders.length === 0) {
      await connection.end();
      console.log(`syncOrderStatus thực thi trong ${Date.now() - startTime}ms`);
      return;
    }

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

    if (results.length > 0) {
      io.emit("overdueOrdersUpdated", {
        message: "Danh sách đơn hàng đã được cập nhật trạng thái",
        updatedCount: results.length,
      });
    }
  } catch (error) {
    console.error("Lỗi trong syncOrderStatus:", error.message);
    throw error;
  }
}

async function updateOrderStatusToCompleted() {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);

    const [orders] = await connection.query(
      `
      SELECT id_order
      FROM orders
      WHERE status = 'Chờ xác nhận giao/lấy hàng'
      `
    );
    console.log(`Số lượng đơn hàng cần kiểm tra trạng thái: ${orders.length}`);

    if (orders.length === 0) {
      console.log("Không có đơn hàng nào cần cập nhật trạng thái.");
      await connection.end();
      console.log(
        `updateOrderStatusToCompleted thực thi trong ${
          Date.now() - startTime
        }ms`
      );
      return;
    }

    const limit = pLimit(10);
    let api2RequestCount = 0;

    const statusPromises = orders.map((order) =>
      limit(async () => {
        api2RequestCount++;
        try {
          const response = await axios.get(
            `${API_2_BASE}?qc=${order.id_order}`
          );
          return {
            MaPX: order.id_order,
            Tinhtranggiao: response.data.Tinhtranggiao,
          };
        } catch (err) {
          console.error(
            `Lỗi khi gọi API_2 cho id_order ${order.id_order}:`,
            err.message
          );
          return null;
        }
      })
    );

    const settledResults = await Promise.allSettled(statusPromises);
    const results = settledResults
      .filter(
        (result) => result.status === "fulfilled" && result.value !== null
      )
      .map((result) => result.value);

    console.log(
      `Tổng số yêu cầu API_2 trong updateOrderStatusToCompleted: ${api2RequestCount}`
    );

    const completedOrders = results.filter(
      (order) => order.Tinhtranggiao === "Hoàn thành"
    );
    console.log(
      `Số lượng đơn hàng cập nhật thành Hoàn thành: ${completedOrders.length}`
    );

    if (completedOrders.length > 0) {
      const values = completedOrders.map((order) => ["Hoàn thành", order.MaPX]);

      const [updateResult] = await connection.query(
        `
        UPDATE orders
        SET status = ?
        WHERE id_order = ?
        `,
        values.flat()
      );
      console.log(
        `Số dòng ảnh hưởng khi cập nhật trạng thái Hoàn thành: ${updateResult.affectedRows}`
      );
    }

    await connection.end();
    console.log(
      `updateOrderStatusToCompleted thực thi trong ${Date.now() - startTime}ms`
    );
  } catch (error) {
    console.error("Lỗi trong updateOrderStatusToCompleted:", error.message);
    throw error;
  }
}

async function analyzeDeliveryNote() {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const [orders] = await connection.query(
      `
      SELECT o.id_order, o.delivery_note, o.date_delivery, oa.travel_time
      FROM orders o
      LEFT JOIN orders_address oa ON o.id_order = o.id_order
      WHERE o.delivery_note IS NOT NULL
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
        AND o.priority = 0
        AND o.delivery_deadline IS NULL
      `
    );
    console.log(`Số lượng đơn hàng có ghi chú: ${orders.length}`);

    if (orders.length === 0) {
      await connection.end();
      console.log(
        `analyzeDeliveryNote thực thi trong ${Date.now() - startTime}ms`
      );
      return;
    }

    const priorityOrders = [];
    const limit = pLimit(5);

    // Hàm chuẩn hóa ghi chú
    const normalizeNote = (note) => {
      if (!note) return "";
      return note
        .toLowerCase()
        .replace(/trc|truoc/g, "trước")
        .replace(/gap/g, "gấp")
        .replace(/sn|sớm nhất|sớm nhé/g, "sớm")
        .replace(/nhah|nhan|nhanh len|nhanh nha/g, "nhanh")
        .replace(/sang/g, "sáng")
        .replace(/chiu|chiu nay/g, "chiều")
        .replace(/toi|toi nay/g, "tối")
        .replace(/hom nay|hnay/g, "hôm nay")
        .replace(/mai|ngay mai/g, "ngày mai")
        .replace(/mot|ngay mot/g, "ngày mốt")
        .replace(/\s+/g, " ")
        .trim();
    };

    const parseDeliveryNote = (note, travelTime, order) => {
      const deliveryTime = moment(
        order.date_delivery,
        "DD/MM/YYYY HH:mm:ss"
      ).tz("Asia/Ho_Chi_Minh");
      const isSaturday = deliveryTime.day() === 6;
      let deliveryDeadline = null;
      let priority = 0;

      if (!deliveryTime.isValid()) {
        console.warn(
          `Invalid date_delivery for id_order ${order.id_order}: ${order.date_delivery}`
        );
        return {
          id_order: order.id_order,
          delivery_deadline: null,
          priority: 0,
        };
      }

      const normalizedNote = normalizeNote(note);

      // Trường hợp gấp/khẩn cấp
      const urgentRegex =
        /(gấp|nhanh|sớm|liền|ngay lập tức|khẩn cấp|urgent|hỏa tốc|mau lên|nhanh nhất|sáng sớm)/i;
      if (urgentRegex.test(normalizedNote)) {
        deliveryDeadline = deliveryTime.clone().add(travelTime + 15, "minutes");
        priority = 2;
      } else {
        // Trường hợp chỉ định thời gian cụ thể
        const specificTimeRegex =
          /trước\s*(?:(\d{1,2}(?::\d{2})?(?:h|pm|am)?)|ăn trưa|ăn tối|(\d{1,2}h\d{2}))(?:\s*(sáng|chiều))?/i;
        const specificMatch = normalizedNote.match(specificTimeRegex);
        if (specificMatch) {
          if (specificMatch[1] || specificMatch[2]) {
            let timeStr =
              specificMatch[1] || specificMatch[2].replace("h", ":");
            if (!timeStr.includes(":")) timeStr += ":00";
            deliveryDeadline = deliveryTime
              .clone()
              .startOf("day")
              .add(moment.duration(timeStr));
            if (specificMatch[3] === "chiều" && deliveryDeadline.hour() < 12) {
              deliveryDeadline.add(12, "hours");
            } else if (
              specificMatch[3] === "sáng" &&
              deliveryDeadline.hour() >= 12
            ) {
              deliveryDeadline.subtract(12, "hours");
            }
            const timeToDeadline = deliveryDeadline.diff(
              deliveryTime,
              "minutes"
            );
            priority = timeToDeadline <= (travelTime + 15) * 1.5 ? 2 : 1;
          } else if (specificMatch[0].includes("ăn trưa")) {
            deliveryDeadline = deliveryTime
              .clone()
              .startOf("day")
              .add(11, "hours")
              .add(45, "minutes");
            priority = 1;
          } else if (specificMatch[0].includes("ăn tối")) {
            deliveryDeadline = deliveryTime
              .clone()
              .startOf("day")
              .add(17, "hours")
              .add(25, "minutes");
            priority = 1;
          }
        } else {
          // Trường hợp ghi chú mơ hồ
          const vagueRegex =
            /(sáng nay|chiều nay|tối nay|hôm nay|trong ngày|sáng mai|chiều mai|tối mai|ngày mai|ngày mốt|ngày kia|thứ hai|tuần sau|đầu giờ|đầu giờ chiều|cuối giờ|sáng (\d+) ngày nữa)/i;
          const vagueMatch = normalizedNote.match(vagueRegex);
          if (vagueMatch) {
            switch (vagueMatch[0].toLowerCase()) {
              case "sáng nay":
                deliveryDeadline = deliveryTime
                  .clone()
                  .startOf("day")
                  .add(10, "hours");
                priority =
                  deliveryDeadline.diff(deliveryTime, "minutes") <= 60 ? 2 : 1;
                break;
              case "chiều nay":
                deliveryDeadline = deliveryTime
                  .clone()
                  .startOf("day")
                  .add(15, "hours");
                priority =
                  deliveryDeadline.diff(deliveryTime, "minutes") <= 60 ? 2 : 1;
                break;
              case "tối nay":
              case "hôm nay":
              case "trong ngày":
                deliveryDeadline = deliveryTime
                  .clone()
                  .startOf("day")
                  .add(17, "hours")
                  .add(40, "minutes");
                priority =
                  deliveryDeadline.diff(deliveryTime, "minutes") <= 90 ? 2 : 1;
                break;
              case "sáng mai":
              case "ngày mai":
                deliveryDeadline = deliveryTime
                  .clone()
                  .add(1, "day")
                  .startOf("day")
                  .add(10, "hours");
                priority = 1;
                break;
              case "chiều mai":
                deliveryDeadline = deliveryTime
                  .clone()
                  .add(1, "day")
                  .startOf("day")
                  .add(15, "hours");
                priority = 1;
                break;
              case "tối mai":
                deliveryDeadline = deliveryTime
                  .clone()
                  .add(1, "day")
                  .startOf("day")
                  .add(17, "hours")
                  .add(40, "minutes");
                priority = 1;
                break;
              case "ngày mốt":
              case "ngày kia":
                deliveryDeadline = deliveryTime
                  .clone()
                  .add(2, "days")
                  .startOf("day")
                  .add(10, "hours");
                priority = 1;
                break;
              case "thứ hai":
                deliveryDeadline = deliveryTime
                  .clone()
                  .startOf("week")
                  .add(1, "week")
                  .startOf("day")
                  .add(10, "hours");
                if (deliveryTime.day() === 0) deliveryDeadline.add(1, "day");
                priority = 1;
                break;
              case "tuần sau":
                deliveryDeadline = deliveryTime
                  .clone()
                  .startOf("week")
                  .add(1, "week")
                  .startOf("day")
                  .add(10, "hours");
                priority = 1;
                break;
              case "đầu giờ":
                deliveryDeadline = deliveryTime
                  .clone()
                  .startOf("day")
                  .add(8, "hours")
                  .add(travelTime + 15, "minutes");
                priority = 1;
                break;
              case "đầu giờ chiều":
                deliveryDeadline = deliveryTime
                  .clone()
                  .startOf("day")
                  .add(13, "hours")
                  .add(30, "minutes");
                priority = 1;
                break;
              case "cuối giờ":
                deliveryDeadline = deliveryTime
                  .clone()
                  .startOf("day")
                  .add(17, "hours")
                  .add(40, "minutes");
                priority =
                  deliveryDeadline.diff(deliveryTime, "minutes") <= 90 ? 2 : 1;
                break;
              default:
                if (vagueMatch[1]) {
                  const days = parseInt(vagueMatch[1], 10);
                  deliveryDeadline = deliveryTime
                    .clone()
                    .add(days, "days")
                    .startOf("day")
                    .add(10, "hours");
                  priority = 1;
                }
                break;
            }
          }
        }
      }

      if (deliveryDeadline) {
        const startOfDay = deliveryDeadline.clone().startOf("day");
        const workStart = startOfDay.clone().add(8, "hours");
        const workEnd = isSaturday
          ? startOfDay.clone().add(16, "hours").add(30, "minutes")
          : startOfDay.clone().add(17, "hours").add(40, "minutes");
        const lunchStart = startOfDay.clone().add(12, "hours");
        const lunchEnd = startOfDay.clone().add(13, "hours").add(30, "minutes");

        // Xử lý thời gian nghỉ trưa (12:00 - 13:30)
        if (
          deliveryDeadline.isSameOrAfter(lunchStart) &&
          deliveryDeadline.isBefore(lunchEnd)
        ) {
          deliveryDeadline = lunchEnd.clone();
          priority =
            deliveryDeadline.diff(deliveryTime, "minutes") <= 90 ? 2 : 1;
        }

        // Đảm bảo deadline không trước giờ làm việc hoặc sau giờ kết thúc
        if (deliveryDeadline.isBefore(workStart)) {
          deliveryDeadline = workStart.clone();
          priority = 1;
        } else if (deliveryDeadline.isAfter(workEnd)) {
          deliveryDeadline = workEnd.clone();
          priority = 1;
        }

        // Nếu deadline trước thời gian hiện tại, đặt thành thời gian sớm nhất có thể
        if (deliveryDeadline.isBefore(deliveryTime)) {
          deliveryDeadline = deliveryTime
            .clone()
            .add(travelTime + 15, "minutes");
          priority = 2;
        }

        return {
          id_order: order.id_order,
          delivery_deadline: deliveryDeadline.format("YYYY-MM-DD HH:mm:ss"),
          priority,
        };
      }

      return {
        id_order: order.id_order,
        delivery_deadline: null,
        priority: 0,
      };
    };

    const parsePromises = orders.map((order) =>
      limit(async () => {
        const result = parseDeliveryNote(
          order.delivery_note,
          order.travel_time || 15,
          order
        ); // Default travel_time = 15 nếu NULL
        console.log(
          `Kết quả phân tích cho id_order ${order.id_order}:`,
          result
        );
        if (result.priority > 0 || result.delivery_deadline) {
          priorityOrders.push([
            result.priority,
            result.delivery_deadline,
            order.id_order,
          ]);
        }
        return result;
      })
    );

    await Promise.all(parsePromises);

    if (priorityOrders.length > 0) {
      for (const [priority, deliveryDeadline, idOrder] of priorityOrders) {
        try {
          const [updateResult] = await connection.query(
            `
            UPDATE orders
            SET priority = ?, delivery_deadline = ?
            WHERE id_order = ?
            `,
            [priority, deliveryDeadline, idOrder]
          );
          console.log(
            `Cập nhật thành công id_order ${idOrder}: priority=${priority}, delivery_deadline=${deliveryDeadline}`
          );
        } catch (error) {
          console.error(`Lỗi khi cập nhật id_order ${idOrder}:`, error.message);
        }
      }
    } else {
      console.log("Không có đơn hàng nào để cập nhật");
    }

    await connection.end();
    console.log(
      `analyzeDeliveryNote thực thi trong ${Date.now() - startTime}ms`
    );
  } catch (error) {
    console.error("Lỗi trong analyzeDeliveryNote:", error.message);
    throw error;
  }
}

async function main(page = 1, io) {
  const startTime = Date.now();
  try {
    console.log(
      "🚀 Khởi động công cụ giao hàng lúc:",
      moment().tz("Asia/Ho_Chi_Minh").format()
    );
    console.log(
      "================================================================="
    );

    console.log("📋 Bước 1: Cập nhật trạng thái đơn hàng...");
    await updateOrderStatusToCompleted();
    console.log("✅ Đã cập nhật trạng thái các đơn hàng hoàn thành");
    console.log(
      "================================================================="
    );

    console.log("⏫ Bước 2: Cập nhật trạng thái ưu tiên đơn hàng...");
    await updatePriorityStatus(io);
    console.log("✅ Đã cập nhật trạng thái ưu tiên");
    console.log(
      "================================================================="
    );

    console.log("📦 Bước 3: Lấy và lưu đơn hàng...");
    const orders = await fetchAndSaveOrders();
    console.log(`✅ Đã lưu đơn hàng: ${orders.length}`);
    console.log(
      "================================================================="
    );

    console.log("📝 Bước 4: Phân tích ghi chú đơn hàng...");
    await analyzeDeliveryNote();
    console.log("✅ Đã phân tích ghi chú và cập nhật ưu tiên");
    console.log(
      "================================================================="
    );

    if (orders.length === 0) {
      console.log(
        "ℹ️ Không có đơn hàng mới, lấy danh sách đơn hàng hiện có..."
      );
      const groupedOrders = await groupOrders(page);
      console.log(
        "📊 Kết quả đơn hàng:",
        JSON.stringify(groupedOrders, null, 2)
      );
      console.log("🏁 Công cụ giao hàng hoàn tất.");
      console.log(`⏱️ main thực thi trong ${Date.now() - startTime}ms`);

      if (io) {
        io.emit("ordersUpdated", {
          message: "Danh sách đơn hàng đã được cập nhật",
          data: groupedOrders,
          nextRunTime: getNextCronRunTime(),
        });
        console.log(`Đã gửi danh sách đơn hàng và nextRunTime qua Socket.io`);
      }
      return groupedOrders;
    }

    console.log("🗺️ Bước 5: Chuẩn hóa và ánh xạ địa chỉ...");
    const ordersToStandardize = orders.filter(
      (order) => order.addressChanged || order.isEmpty
    );
    console.log(`Số đơn hàng cần chuẩn hóa: ${ordersToStandardize.length}`);
    const standardizedOrders = await standardizeAddresses(ordersToStandardize);
    console.log(
      `✅ Đã chuẩn hóa và ánh xạ đơn hàng: ${standardizedOrders.length}`
    );
    console.log(
      "================================================================="
    );

    console.log("💾 Bước 6: Cập nhật địa chỉ chuẩn hóa...");
    await updateStandardizedAddresses(standardizedOrders);
    console.log("✅ Đã cập nhật địa chỉ chuẩn hóa");
    console.log(
      "================================================================="
    );

    console.log("📏 Bước 7: Tính toán khoảng cách và thời gian...");
    await calculateDistances();
    console.log("✅ Đã tính toán khoảng cách và thời gian");
    console.log(
      "================================================================="
    );

    console.log(`🔍 Bước 8: Lấy đơn hàng gần nhất (trang ${page})...`);
    const groupedOrders = await groupOrders(page);
    console.log(
      "================================================================="
    );

    if (io) {
      io.emit("ordersUpdated", {
        message: "Danh sách đơn hàng đã được cập nhật",
        data: groupedOrders,
        nextRunTime: getNextCronRunTime(),
      });
      console.log(`Đã gửi danh sách đơn hàng và nextRunTime qua Socket.io`);
    }

    console.log("🏁 Công cụ giao hàng hoàn tất.");
    console.log(`⏱️ main thực thi trong ${Date.now() - startTime}ms`);
    return groupedOrders;
  } catch (error) {
    console.error("❌ Lỗi trong main:", error.message);
    throw error;
  }
}

// Chạy main ngay khi khởi động server
main(1, io).catch((error) =>
  console.error("Lỗi khi chạy main lần đầu:", error.message)
);

// Lập lịch chạy tự động mỗi 5 phút
cron.schedule("*/5 * * * *", () => {
  console.log(
    "Chạy quy trình giao hàng lúc:",
    moment().tz("Asia/Ho_Chi_Minh").format()
  );
  main(1, io).catch((error) =>
    console.error("Lỗi khi chạy main:", error.message)
  );
});

// Lập lịch đồng bộ trạng thái mỗi 15 phút
cron.schedule("*/15 * * * *", () => {
  console.log(
    "Chạy quy trình đồng bộ trạng thái lúc:",
    moment().tz("Asia/Ho_Chi_Minh").format()
  );
  syncOrderStatus().catch((error) =>
    console.error("Lỗi khi chạy syncOrderStatus:", error.message)
  );
});

// --------------------------------------------- ROUTE ---------------------------------------------
app.get("/grouped-orders", async (req, res) => {
  try {
    console.time("grouped-orders");
    const page = parseInt(req.query.page) || 1;
    const filterDate = req.query.date || null;

    if (isNaN(page) || page < 1) {
      return res.status(400).json({ error: "Page phải là số nguyên dương" });
    }

    console.log(
      `Gọi groupOrders với page: ${page}, date: ${filterDate || "all"}`
    );
    const groupedOrders = await groupOrders(page, filterDate);

    console.timeEnd("grouped-orders");
    res.status(200).json(groupedOrders);
  } catch (error) {
    console.error("Lỗi trong /grouped-orders:", error.message, error.stack);
    res.status(500).json({ error: "Lỗi server", details: error.message });
  }
});

app.get("/grouped-orders2", async (req, res) => {
  try {
    console.time("grouped-orders2");
    const page = parseInt(req.query.page) || 1;
    const filterDate = req.query.date || null;

    if (isNaN(page) || page < 1) {
      return res.status(400).json({ error: "Page phải là số nguyên dương" });
    }

    console.log(
      `Gọi groupOrders với page: ${page}, date: ${filterDate || "all"}`
    );
    const groupedOrders = await groupOrders2(page, filterDate);

    console.timeEnd("grouped-orders2");
    res.status(200).json(groupedOrders);
  } catch (error) {
    console.error("Lỗi trong /grouped-orders2:", error.message, error.stack);
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

app.get("/locations", async (req, res) => {
  const connection = await mysql.createConnection(dbConfig);
  const [rows] = await connection.query(`
    SELECT DISTINCT district, ward
    FROM orders_address
    WHERE district IS NOT NULL AND ward IS NOT NULL
  `);

  const districts = [...new Set(rows.map((r) => r.district.trim()))];
  const wards = [...new Set(rows.map((r) => r.ward.trim()))];

  res.json({
    districts,
    wards,
    mapping: rows.map((r) => ({
      district: r.district.trim(),
      ward: r.ward.trim(),
    })),
  });
});

app.get("/orders/search", async (req, res) => {
  const { date = null, keyword = "", type = "district" } = req.query;

  if (!keyword.trim()) {
    return res.status(400).json({ error: "Thiếu giá trị để tìm kiếm." });
  }

  if (!["district", "ward"].includes(type)) {
    return res.status(400).json({ error: "Tham số type không hợp lệ." });
  }

  try {
    const connection = await mysql.createConnection(dbConfig);

    let dateCondition = "";
    const values = [keyword];

    if (date) {
      if (!moment(date, "YYYY-MM-DD", true).isValid()) {
        return res
          .status(400)
          .json({ error: "Định dạng ngày không hợp lệ, sử dụng YYYY-MM-DD" });
      }
      dateCondition = "AND DATE(o.created_at) = ?";
      values.unshift(date);
    }

    const field = type === "district" ? "a.district" : "a.ward";

    const [rows] = await connection.query(
      `
      SELECT o.*, a.address, a.district, a.ward, a.distance, a.travel_time, a.status AS address_status
      FROM orders o
      LEFT JOIN orders_address a ON o.id_order = a.id_order
      WHERE ${field} = ?
        ${dateCondition}
      ORDER BY o.created_at DESC
      `,
      values
    );

    await connection.end();
    res.json({ orders: rows });
  } catch (err) {
    console.error("Lỗi khi tìm kiếm:", err.message);
    res.status(500).json({ error: "Lỗi server khi tìm kiếm đơn hàng." });
  }
});

app.get("/orders/filter", async (req, res) => {
  const { day = "today", district = "", ward = "" } = req.query;

  if (!district || !ward) {
    return res.status(400).json({ error: "Thiếu quận hoặc phường." });
  }

  try {
    const connection = await mysql.createConnection(dbConfig);

    let dateCondition = "DATE(o.created_at) = CURDATE()";
    if (day === "yesterday") {
      dateCondition = "DATE(o.created_at) = CURDATE() - INTERVAL 1 DAY";
    } else if (day === "older") {
      dateCondition = "DATE(o.created_at) < CURDATE() - INTERVAL 1 DAY";
    }

    const [rows] = await connection.query(
      `
      SELECT o.*, a.address, a.district, a.ward, a.distance, a.travel_time, a.status AS address_status
      FROM orders o
      LEFT JOIN orders_address a ON o.id_order = a.id_order
      WHERE ${dateCondition}
        AND a.district = ?
        AND a.ward = ?
      ORDER BY o.created_at DESC
      `,
      [district, ward]
    );

    await connection.end();
    res.json({ orders: rows });
  } catch (err) {
    console.error("Lỗi khi lọc:", err.message);
    res.status(500).json({ error: "Lỗi server khi lọc đơn hàng." });
  }
});

app.get("/orders/filter-advanced", async (req, res) => {
  const { date = null, districts = "", wards = "" } = req.query;

  const districtList = districts
    ? districts.split(",").map((d) => d.trim())
    : [];
  const wardList = wards ? wards.split(",").map((w) => w.trim()) : [];

  if (districtList.length === 0 && wardList.length === 0) {
    return res.status(400).json({ error: "Thiếu quận hoặc phường để lọc." });
  }

  try {
    const connection = await mysql.createConnection(dbConfig);

    let dateCondition = "";
    const values = [];

    if (date) {
      if (!moment(date, "YYYY-MM-DD", true).isValid()) {
        return res
          .status(400)
          .json({ error: "Định dạng ngày không hợp lệ, sử dụng YYYY-MM-DD" });
      }
      dateCondition = "DATE(a.created_at) = ?";
      values.push(date);
    }

    const filters = dateCondition ? [dateCondition] : [];
    if (districtList.length > 0) {
      filters.push(`a.district IN (${districtList.map(() => "?").join(",")})`);
      values.push(...districtList);
    }

    if (wardList.length > 0) {
      filters.push(`a.ward IN (${wardList.map(() => "?").join(",")})`);
      values.push(...wardList);
    }

    const whereClause = filters.join(" AND ") || "1=1";

    const [rows] = await connection.query(
      `
      SELECT 
        o.*, 
        a.address, a.district, a.ward, 
        a.distance, a.travel_time, 
        a.created_at AS address_created_at,
        a.status AS address_status
      FROM orders o
      LEFT JOIN orders_address a ON o.id_order = a.id_order
      WHERE ${whereClause}
      ORDER BY a.created_at DESC
      `,
      values
    );

    await connection.end();

    res.json({ orders: rows });
  } catch (err) {
    console.error("Lỗi /orders/filter-advanced:", err.message, err.stack);
    res.status(500).json({ error: "Lỗi server khi lọc nâng cao." });
  }
});

app.get("/orders/filter-by-date", async (req, res) => {
  const startTime = Date.now();
  try {
    const { page = 1, filterDate } = req.query;
    const pageNum = parseInt(page);

    if (!Number.isInteger(pageNum) || pageNum < 1) {
      return res.status(400).json({ error: "Page phải là số nguyên dương" });
    }

    if (filterDate && !moment(filterDate, "YYYY-MM-DD", true).isValid()) {
      return res
        .status(400)
        .json({ error: "filterDate phải có định dạng YYYY-MM-DD" });
    }

    console.log(
      `API received: page=${page}, filterDate=${filterDate || "all"}`
    );

    const connection = await mysql.createConnection(dbConfig);
    const pageSize = 10;
    const offset = (pageNum - 1) * pageSize;

    const countQuery = `
      SELECT COUNT(*) as total
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.address IS NOT NULL 
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
        ${
          filterDate
            ? "AND DATE(CONVERT_TZ(o.created_at, '+00:00', '+07:00')) = ?"
            : ""
        }
    `;
    const countParams = filterDate ? [filterDate] : [];
    const [totalResult] = await connection.execute(countQuery, countParams);

    const totalOrders = totalResult[0].total;
    console.log(`Tổng số đơn hàng: ${totalOrders}`);
    const totalPages = Math.ceil(totalOrders / pageSize);

    const query = `
      SELECT 
        oa.id_order,
        oa.address,
        oa.source,
        oa.distance,
        oa.travel_time,
        oa.status,
        oa.district,
        oa.ward,
        o.created_at,
        o.SOKM,
        o.priority,
        o.delivery_deadline,
        o.delivery_note,
        CASE 
          WHEN DATE(CONVERT_TZ(o.created_at, '+00:00', '+07:00')) <= CURDATE() - INTERVAL 2 DAY THEN 2
          WHEN DATE(CONVERT_TZ(o.created_at, '+00:00', '+07:00')) = CURDATE() - INTERVAL 1 DAY THEN 1 
          ELSE 0 
        END AS days_old
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.address IS NOT NULL 
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
        ${
          filterDate
            ? "AND DATE(CONVERT_TZ(o.created_at, '+00:00', '+07:00')) = ?"
            : ""
        }
      ORDER BY
        CASE
          WHEN oa.district IS NULL OR oa.ward IS NULL OR oa.distance IS NULL OR oa.travel_time IS NULL THEN 100
          WHEN o.priority = 2 THEN 0
          WHEN oa.status = 1 AND o.priority = 1 AND o.delivery_deadline IS NOT NULL
               AND o.delivery_deadline <= NOW() + INTERVAL 2 HOUR THEN 1
          WHEN days_old = 2 AND oa.status = 1 AND o.delivery_deadline IS NOT NULL
               AND o.delivery_deadline <= NOW() + INTERVAL 2 HOUR THEN 2
          WHEN days_old = 2 AND oa.status = 0 AND o.delivery_deadline IS NOT NULL
               AND o.delivery_deadline <= NOW() + INTERVAL 2 HOUR THEN 3
          WHEN days_old = 1 AND oa.status = 1 AND o.delivery_deadline IS NOT NULL
               AND o.delivery_deadline <= NOW() + INTERVAL 2 HOUR THEN 4
          WHEN days_old = 1 AND oa.status = 0 AND o.delivery_deadline IS NOT NULL
               AND o.delivery_deadline <= NOW() + INTERVAL 2 HOUR THEN 5
          WHEN oa.status = 1 AND o.priority = 0 THEN 10
          WHEN oa.status = 1 AND o.priority = 1 
               AND (o.delivery_deadline IS NULL OR o.delivery_deadline > NOW() + INTERVAL 2 HOUR) THEN 11
          WHEN oa.status = 0 AND o.priority = 1 
               AND (o.delivery_deadline IS NULL OR o.delivery_deadline > NOW() + INTERVAL 2 HOUR) THEN 12
          WHEN oa.status = 0 AND o.priority = 0 THEN 13
          WHEN days_old = 2 AND (o.delivery_deadline IS NULL OR o.delivery_deadline > NOW() + INTERVAL 2 HOUR) THEN 14
          WHEN days_old = 1 AND (o.delivery_deadline IS NULL OR o.delivery_deadline > NOW() + INTERVAL 2 HOUR) THEN 15
          ELSE 16
        END ASC,
        CASE 
          WHEN DATE(o.delivery_deadline) = CURDATE() THEN 0
          ELSE 1
        END ASC,
        CASE 
          WHEN o.delivery_deadline IS NOT NULL THEN TIMESTAMPDIFF(MINUTE, NOW(), o.delivery_deadline)
          ELSE 999999
        END ASC,
        COALESCE(oa.distance, 999999) ASC,
        COALESCE(oa.travel_time, 999999) ASC,
        o.created_at ASC
      LIMIT ${pageSize} OFFSET ${offset}
    `;
    const queryParams = filterDate ? [filterDate] : [];
    const [results] = await connection.execute(query, queryParams);
    console.log(`Số đơn trả về: ${results.length}`);

    const parsedResults = results.map((row) => ({
      id_order: row.id_order,
      address: row.address,
      source: row.source,
      distance: row.distance,
      travel_time: row.travel_time,
      status: row.status,
      created_at: row.created_at
        ? moment(row.created_at)
            .tz("Asia/Ho_Chi_Minh")
            .format("YYYY-MM-DD HH:mm:ss")
        : null,
      SOKM: row.SOKM,
      priority: row.priority,
      delivery_deadline: row.delivery_deadline
        ? moment(row.delivery_deadline)
            .tz("Asia/Ho_Chi_Minh")
            .format("YYYY-MM-DD HH:mm:ss")
        : null,
      delivery_note: row.delivery_note,
      district: row.district || null,
      ward: row.ward || null,
      days_old: row.days_old,
    }));

    await connection.end();

    res.json({
      totalOrders,
      totalPages,
      currentPage: pageNum,
      lastRun: moment().tz("Asia/Ho_Chi_Minh").format(),
      orders: parsedResults,
    });
  } catch (error) {
    console.error("API error:", error.message, error.stack);
    res.status(500).json({ error: `Không thể lọc đơn hàng: ${error.message}` });
  }
});

app.get("/orders/search-by-id", async (req, res) => {
  const { keyword = "", date = null } = req.query;

  if (!keyword.trim()) {
    return res.status(400).json({ error: "Thiếu mã đơn hàng để tìm kiếm." });
  }

  try {
    const connection = await mysql.createConnection(dbConfig);

    let dateCondition = "";
    const values = [`%${keyword}%`];

    if (date) {
      if (!moment(date, "YYYY-MM-DD", true).isValid()) {
        return res
          .status(400)
          .json({ error: "Định dạng ngày không hợp lệ, sử dụng YYYY-MM-DD" });
      }
      dateCondition = "AND DATE(o.created_at) = ?";
      values.push(date);
    }

    const [rows] = await connection.query(
      `
      SELECT o.*, a.address, a.district, a.ward, a.distance, a.travel_time, a.status AS address_status
      FROM orders o
      LEFT JOIN orders_address a ON o.id_order = a.id_order
      WHERE o.id_order LIKE ?
        ${dateCondition}
      ORDER BY o.created_at DESC
      `,
      values
    );

    await connection.end();
    res.json({ orders: rows });
  } catch (err) {
    console.error("Lỗi khi tìm kiếm đơn hàng:", err.message);
    res.status(500).json({ error: "Lỗi server khi tìm kiếm đơn hàng." });
  }
});

app.get("/orders/overdue", async (req, res) => {
  try {
    const connection = await mysql.createConnection(dbConfig);

    const [rows] = await connection.query(
      `
      SELECT o.*, a.address, a.district, a.ward, a.distance, a.travel_time, a.status AS address_status
      FROM orders o
      LEFT JOIN orders_address a ON o.id_order = a.id_order
      WHERE a.status = 1
      ORDER BY o.created_at DESC
      `
    );

    await connection.end();
    res.json({ orders: rows });
  } catch (err) {
    console.error("Lỗi khi lấy đơn hàng quá hạn:", err.message);
    res.status(500).json({ error: "Lỗi server khi lấy đơn hàng quá hạn." });
  }
});

app.get("/orders/find-by-id", async (req, res) => {
  const { id = "", date = null } = req.query;

  if (!id.trim()) {
    return res.status(400).json({ error: "Thiếu mã đơn hàng để tìm kiếm." });
  }

  try {
    const connection = await mysql.createConnection(dbConfig);

    let dateCondition = "";
    const values = [id];

    if (date) {
      if (!moment(date, "YYYY-MM-DD", true).isValid()) {
        return res
          .status(400)
          .json({ error: "Định dạng ngày không hợp lệ, sử dụng YYYY-MM-DD" });
      }
      dateCondition = "AND DATE(o.created_at) = ?";
      values.push(date);
    }

    const [rows] = await connection.query(
      `
      SELECT o.*, a.address, a.district, a.ward, a.distance, a.travel_time, a.status AS address_status
      FROM orders o
      LEFT JOIN orders_address a ON o.id_order = a.id_order
      WHERE o.id_order = ?
        ${dateCondition}
      ORDER BY o.created_at DESC
      LIMIT 1
      `,
      values
    );

    await connection.end();

    if (rows.length === 0) {
      return res.status(404).json({ error: "Không tìm thấy đơn hàng." });
    }

    res.json({ order: rows[0] });
  } catch (err) {
    console.error("Lỗi khi tìm kiếm đơn hàng:", err.message);
    res.status(500).json({ error: "Lỗi server khi tìm kiếm đơn hàng." });
  }
});
server.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
