require('dotenv').config();
console.log('OPENAI_API_KEY (delivery-tool.js):', process.env.OPENAI_API_KEY); // Thêm log để kiểm tra

const axios = require("axios");
const mysql = require("mysql2/promise");
const { OpenAI } = require("openai");
const pLimitModule = require("p-limit");

const pLimit =
  typeof pLimitModule === "function" ? pLimitModule : pLimitModule.default;

const dbConfig = {
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "",
  database: process.env.DB_DATABASE || "delivery_data", // Sử dụng DB_DATABASE từ .env
};

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

const TOMTOM_API_KEY = process.env.TOMTOM_API_KEY;

// Kiểm tra API keys
if (!process.env.OPENAI_API_KEY) {
  console.error("Lỗi: Thiếu OPENAI_API_KEY trong biến môi trường");
  process.exit(1);
}
if (!process.env.TOMTOM_API_KEY) {
  console.error("Lỗi: Thiếu TOMTOM_API_KEY trong biến môi trường");
  process.exit(1);
}

const API_1 =
  "http://192.168.117.222:8096/NKC/Delivery/GetVoucher?type=chogiao";
const API_2_BASE = "http://192.168.117.222:8096/NKC/Web/SearchVoucher";

const DEFAULT_ADDRESS = {
  DcGiaohang:
    "108/E8 Đường Cộng Hòa, Phường 4, Quận Tân Bình, Hồ Chí Minh, Việt Nam",
  District: "Quận Tân Bình",
  Ward: "Phường 4",
  Source: "Default",
};

const TRANSPORT_KEYWORDS = ["XE", "CHÀNH XE", "GỬI XE", "NHÀ XE", "XE KHÁCH"];

// Bounding box cho nội thành TP. Hồ Chí Minh
const HCM_BOUNDS = {
  minLon: 106.58,
  minLat: 10.69,
  maxLon: 106.84,
  maxLat: 10.88,
};

// Hàm kiểm tra địa chỉ liên quan đến nhà xe
function isTransportAddress(address) {
  if (!address) return false;
  const lowerAddress = address.toUpperCase();
  return TRANSPORT_KEYWORDS.some((keyword) => lowerAddress.includes(keyword));
}

// Hàm tiền xử lý địa chỉ để loại bỏ thông tin dư thừa
function preprocessAddress(address) {
  if (!address) return "";
  let cleanedAddress = address
    .replace(/\b\d{10,11}\b/g, "") // Loại bỏ số điện thoại (10-11 chữ số)
    .replace(/\([^)]*\)/g, "") // Loại bỏ nội dung trong ngoặc
    .replace(/Chiều *: *Giao trước \d{1,2}(g|h)\d{0,2}/gi, "") // Loại bỏ ghi chú thời gian
    .replace(/[-–/]\s*\w+\s*$/, "") // Loại bỏ tên người ở cuối
    .replace(/\s+/g, " ") // Chuẩn hóa khoảng trắng
    .trim();
  return cleanedAddress;
}

// Hàm kiểm tra địa chỉ hợp lệ
function isValidAddress(address) {
  if (!address || address.trim() === "") return false;
  return true;
}

// Hàm kiểm tra MaPX tồn tại trong bảng orders
async function getValidOrderIds() {
  try {
    const connection = await mysql.createConnection(dbConfig);
    const [rows] = await connection.execute("SELECT id_order FROM orders");
    await connection.end();
    return new Set(rows.map((row) => row.id_order));
  } catch (error) {
    console.error("Lỗi khi lấy danh sách id_order:", error.message);
    return new Set();
  }
}

// Hàm kiểm tra địa chỉ có thông tin cụ thể (số nhà, tên đường)
function hasSpecificAddress(address) {
  return /\d+.*\b(Đường|Phố|Phường|Quận|Huyện)\b/i.test(address);
}

// Hàm gọi TomTom Search API (Fuzzy Search) để geocoding
async function geocodeAddress(address, isTransport = false, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const params = {
        key: TOMTOM_API_KEY,
        countrySet: "VN",
        language: "vi-VN",
        limit: 1,
      };

      if (isTransport) {
        params.minLon = HCM_BOUNDS.minLon;
        params.minLat = HCM_BOUNDS.minLat;
        params.maxLon = HCM_BOUNDS.maxLon;
        params.maxLat = HCM_BOUNDS.maxLat;
      }

      const response = await axios.get(
        "https://api.tomtom.com/search/2/search/" +
          encodeURIComponent(address + ", Việt Nam") +
          ".json",
        { params }
      );

      if (response.data.results && response.data.results.length > 0) {
        const result = response.data.results[0];
        const formattedAddress = result.address.freeformAddress;

        let district = null,
          ward = null;

        const addressComponents = result.address;
        if (addressComponents.municipalitySubdivision) {
          ward = addressComponents.municipalitySubdivision;
        }
        if (addressComponents.municipality) {
          district = addressComponents.municipality;
        }

        return {
          DcGiaohang: formattedAddress + ", Việt Nam",
          District: district,
          Ward: ward,
          Source: "TomTom",
        };
      } else {
        console.warn(
          `TomTom API không tìm thấy kết quả cho địa chỉ: ${address}`
        );
        return null;
      }
    } catch (error) {
      if (i === retries - 1) {
        console.error(`Hết lượt thử cho địa chỉ ${address}:`, error.message);
        return null;
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

async function fetchAndSaveOrders() {
  try {
    const response1 = await axios.get(API_1);
    const orders = response1.data;
    console.log("Số lượng đơn hàng từ API 1:", orders.length);

    const limit = pLimit(10);
    const api2Promises = orders.map((order) =>
      limit(() =>
        axios
          .get(`${API_2_BASE}?qc=${order.MaPX}`)
          .then((res) => ({
            MaPX: order.MaPX,
            DcGiaohang: res.data.DcGiaohang || "",
            isEmpty: !res.data.DcGiaohang,
          }))
          .catch((err) => {
            console.error(
              `Lỗi khi gọi API 2 cho MaPX ${order.MaPX}:`,
              err.message
            );
            return null;
          })
      )
    );

    const settledResults = await Promise.allSettled(api2Promises);
    const results = settledResults
      .filter(
        (result) => result.status === "fulfilled" && result.value !== null
      )
      .map((result) => result.value);
    console.log("Số lượng kết quả thành công từ API 2:", results.length);

    const connection = await mysql.createConnection(dbConfig);
    if (results.length > 0) {
      const values = results.map((order) => [order.MaPX, order.DcGiaohang]);
      const [insertResult] = await connection.query(
        "INSERT INTO orders (id_order, address) VALUES ? ON DUPLICATE KEY UPDATE address = VALUES(address)",
        [values]
      );
      console.log(
        "Số dòng ảnh hưởng khi lưu vào cơ sở dữ liệu (orders):",
        insertResult.affectedRows
      );

      const [savedOrders] = await connection.query(
        "SELECT id_order FROM orders WHERE id_order IN (?)",
        [results.map((order) => order.MaPX)]
      );
      const savedMaPX = new Set(savedOrders.map((order) => order.id_order));

      const validResults = results.filter((order) => savedMaPX.has(order.MaPX));
      console.log("Số lượng đơn hàng hợp lệ sau khi lưu:", validResults.length);
      await connection.end();
      return validResults;
    }

    await connection.end();
    return [];
  } catch (error) {
    console.error("Lỗi trong fetchAndSaveOrders:", error.message);
    throw error;
  }
}

async function standardizeAddresses(orders) {
  try {
    const standardizedOrders = [];
    const limit = pLimit(5);

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
            DcGiaohang: null,
            District: null,
            Ward: null,
            Source: null,
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
        5. Chỉ trả về null cho các trường DcGiaohang, District, Ward nếu địa chỉ không có thông tin cụ thể (ví dụ: chỉ có "Gửi xe Kim Mã" mà không có số nhà, đường, hoặc khu vực).
        6. Nếu địa chỉ thuộc tỉnh/thành phố khác TP. Hồ Chí Minh (ví dụ: Bình Phước, Bình Dương), phân tích đúng tỉnh/thành phố dựa trên thông tin.

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
          return { ...result[0], Source: "OpenAI", isEmpty: false };
        } catch (error) {
          console.error(`Lỗi khi gọi OpenAI cho MaPX ${MaPX}:`, error.message);
          return {
            MaPX,
            DcGiaohang: null,
            District: null,
            Ward: null,
            Source: null,
            isEmpty: false,
          };
        }
      })
    );

    const openAIResults = await Promise.all(openAIPromises);
    standardizedOrders.push(...openAIResults);

    const nullAddresses = openAIResults.filter(
      (result) =>
        (!result.DcGiaohang || !result.District || !result.Ward) &&
        !result.isEmpty
    );

    console.log(`Số lượng địa chỉ cần gọi TomTom: ${nullAddresses.length}`);
    console.log(
      "Danh sách địa chỉ cần gọi TomTom:",
      nullAddresses.map((order) => ({
        MaPX: order.MaPX,
        DcGiaohang: order.DcGiaohang,
      }))
    );

    const tomtomPromises = nullAddresses.map((order) =>
      limit(async () => {
        const { MaPX, DcGiaohang } = order;

        const cleanedAddress = preprocessAddress(DcGiaohang);
        if (!cleanedAddress) {
          return {
            MaPX,
            DcGiaohang: null,
            District: null,
            Ward: null,
            Source: null,
            isEmpty: false,
          };
        }

        if (hasSpecificAddress(cleanedAddress)) {
          console.log(
            `Thử lại OpenAI cho địa chỉ cụ thể: ${cleanedAddress} (MaPX: ${MaPX})`
          );
          const retryPrompt = `
          Chuẩn hóa địa chỉ sau thành định dạng: "[Số nhà, Đường], [Phường/Xã], [Quận/Huyện/Thị xã/Thành phố], [Tỉnh/Thành phố], Việt Nam".
          Loại bỏ thông tin dư thừa và ưu tiên số nhà, tên đường, phường, quận.
          Nếu không xác định được, trả về null.

          Địa chỉ: "${cleanedAddress}"

          Đầu ra:
          \`\`\`json
          {
            "DcGiaohang": "Địa chỉ chuẩn hóa hoặc null",
            "District": "Quận/Huyện/Thị xã/Thành phố hoặc null",
            "Ward": "Phường/Xã hoặc null",
            "Source": "OpenAI"
          }
          \`\`\`
          `;

          try {
            const retryCompletion = await openai.chat.completions.create({
              model: "gpt-4o",
              messages: [{ role: "system", content: retryPrompt }],
            });

            let retryContent = retryCompletion.choices[0].message.content;
            retryContent = retryContent
              .replace(/```json\n?|\n?```/g, "")
              .trim();
            const retryResult = JSON.parse(retryContent);
            if (retryResult.DcGiaohang) {
              return {
                MaPX,
                DcGiaohang: retryResult.DcGiaohang,
                District: retryResult.District,
                Ward: retryResult.Ward,
                Source: "OpenAI",
                isEmpty: false,
              };
            }
          } catch (error) {
            console.error(
              `Lỗi khi thử lại OpenAI cho MaPX ${MaPX}:`,
              error.message
            );
          }
        }

        const isTransport = isTransportAddress(cleanedAddress);
        console.log(
          `Gọi TomTom API cho địa chỉ: ${cleanedAddress} (${
            isTransport ? "Nhà xe, giới hạn TP. HCM" : "Không giới hạn"
          })`
        );
        const geocodedResult = await geocodeAddress(
          cleanedAddress,
          isTransport
        );
        if (geocodedResult) {
          return {
            MaPX,
            DcGiaohang: geocodedResult.DcGiaohang || null,
            District: geocodedResult.District || null,
            Ward: geocodedResult.Ward || null,
            Source: geocodedResult.Source || "TomTom",
            isEmpty: false,
          };
        }

        return { ...order, Source: null, isEmpty: false };
      })
    );

    const tomtomResults = await Promise.all(tomtomPromises);

    for (const tomtomResult of tomtomResults) {
      const index = standardizedOrders.findIndex(
        (order) => order.MaPX === tomtomResult.MaPX
      );
      if (index !== -1) {
        standardizedOrders[index] = tomtomResult;
      }
    }

    console.log(
      "Đã chuẩn hóa địa chỉ:",
      JSON.stringify(standardizedOrders, null, 2)
    );
    return standardizedOrders;
  } catch (error) {
    console.error("Lỗi trong standardizeAddresses:", error.message);
    throw error;
  }
}

async function updateStandardizedAddresses(data, isTransport = false) {
  try {
    const connection = await mysql.createConnection(dbConfig);

    if (isTransport) {
      // Lưu vào bảng transport_companies
      const values = data.map((company) => [
        company.name,
        company.address,
        company.standardizedAddress,
        company.district,
        company.ward,
        company.phone,
        company.departureTime,
        company.status,
        company.note,
        company.source,
      ]);

      const [result] = await connection.query(
        `INSERT INTO transport_companies 
        (name, address, standardized_address, district, ward, phone, departure_time, status, note, source) 
        VALUES ? 
        ON DUPLICATE KEY UPDATE 
        address = VALUES(address), 
        standardized_address = VALUES(standardized_address), 
        district = VALUES(district), 
        ward = VALUES(ward), 
        phone = VALUES(phone), 
        departure_time = VALUES(departure_time), 
        status = VALUES(status), 
        note = VALUES(note), 
        source = VALUES(source)`,
        [values]
      );

      console.log(
        "Số dòng ảnh hưởng khi lưu vào cơ sở dữ liệu (transport_companies):",
        result.affectedRows
      );
    } else {
      // Lưu vào bảng orders_address
      const validOrderIds = await getValidOrderIds();
      const validOrders = data.filter((order) =>
        validOrderIds.has(order.MaPX)
      );

      console.log(
        `Số lượng đơn hàng hợp lệ để lưu vào orders_address: ${validOrders.length}`
      );
      if (validOrders.length === 0) {
        console.warn("Không có đơn hàng hợp lệ để lưu vào orders_address");
        await connection.end();
        return;
      }

      const values = validOrders.map((order) => [
        order.MaPX,
        order.DcGiaohang,
        order.District,
        order.Ward,
        order.Source,
      ]);

      const [result] = await connection.query(
        "INSERT INTO orders_address (id_order, address, district, ward, source) VALUES ? " +
          "ON DUPLICATE KEY UPDATE address = VALUES(address), district = VALUES(district), ward = VALUES(ward), source = VALUES(source)",
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
    }

    await connection.end();
  } catch (error) {
    console.error("Lỗi trong updateStandardizedAddresses:", error.message);
    throw error;
  }
}

async function groupOrders() {
  try {
    const connection = await mysql.createConnection(dbConfig);

    const [results] = await connection.execute(`
      SELECT 
        district,
        JSON_ARRAYAGG(
          JSON_OBJECT(
            'ward', ward,
            'orders', COALESCE((
              SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                  'id_order', id_order,
                  'address', address,
                  'source', source
                )
              )
              FROM orders_address sub
              WHERE sub.district = oa.district 
                AND sub.ward = oa.ward
                AND sub.id_order IS NOT NULL
                AND sub.address IS NOT NULL
            ), JSON_ARRAY())
          )
        ) as wards
      FROM (
        SELECT DISTINCT district, ward
        FROM orders_address
        WHERE district IS NOT NULL 
          AND ward IS NOT NULL
      ) oa
      GROUP BY district
    `);

    const parsedResults = results.map((result) => {
      let wards = result.wards;
      if (typeof wards === "string") {
        try {
          wards = JSON.parse(wards);
        } catch (error) {
          console.error(`Lỗi phân tích JSON cho quận ${result.district}:`, error.message);
          wards = [];
        }
      }
      wards = wards.filter((ward) => ward.orders && ward.orders.length > 0);

      return {
        district: result.district,
        wards: wards,
      };
    }).filter((district) => district.wards.length > 0);

    await connection.end();
    console.log("Số lượng quận có đơn hàng:", parsedResults.length);
    return parsedResults;
  } catch (error) {
    console.error("Lỗi trong groupOrders:", error.message);
    throw error;
  }
}

async function main() {
  try {
    console.log("Khởi động công cụ giao hàng...");

    console.log("Bước 1: Lấy và lưu đơn hàng...");
    const orders = await fetchAndSaveOrders();
    console.log("Đã lưu đơn hàng:", orders.length);

    console.log("Bước 2: Chuẩn hóa địa chỉ...");
    const standardizedOrders = await standardizeAddresses(orders);
    console.log("Đã chuẩn hóa đơn hàng:", standardizedOrders.length);

    console.log("Bước 3: Cập nhật địa chỉ chuẩn hóa...");
    await updateStandardizedAddresses(standardizedOrders);
    console.log("Đã cập nhật địa chỉ chuẩn hóa");

    console.log("Bước 4: Nhóm đơn hàng...");
    const groupedOrders = await groupOrders();
    console.log("Đã nhóm đơn hàng:", JSON.stringify(groupedOrders, null, 2));

    console.log("Công cụ giao hàng hoàn tất.");
    return groupedOrders;
  } catch (error) {
    console.error("Lỗi trong main:", error.message);
    throw error;
  }
}

module.exports = { main, groupOrders, standardizeAddresses, updateStandardizedAddresses };