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
const TOMTOM_API_KEY = process.env.TOMTOM_API_KEY;
const WAREHOUSE_ADDRESS = process.env.WAREHOUSE_ADDRESS;

const TRANSPORT_KEYWORDS = ["XE", "CHÀNH XE", "GỬI XE", "NHÀ XE", "XE KHÁCH"];

// ========================================================= UTILITY FUNCTIONS =========================================================
// RETRY FUNCTION
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
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

// RETRY MYSQL CONNECTION
async function createConnectionWithRetry() {
  return await retry(async () => await mysql.createConnection(dbConfig));
}

// ========================================================== ADDRESS & TRANSPORT =========================================================

// PHÂN TÍCH THỜI GIAN KHỞI HÀNH
function parseDepartureTime(departureTime) {
  if (!departureTime) return { start: null, end: null };

  const timeRegex = /^(\d{1,2})[H:](\d{2})?(?:-(\d{1,2})[H:](\d{2})?)?$/i;
  const match = departureTime.match(timeRegex);

  if (!match) return { start: null, end: null };

  const startHour = parseInt(match[1], 10);
  const startMinute = match[2] ? parseInt(match[2], 10) : 0;
  const endHour = match[3] ? parseInt(match[3], 10) : null;
  const endMinute = match[4] ? parseInt(match[4], 10) : 0;

  const start = moment()
    .tz("Asia/Ho_Chi_Minh")
    .startOf("day")
    .add(startHour, "hours")
    .add(startMinute, "minutes");

  let end;
  if (endHour !== null) {
    end = moment()
      .tz("Asia/Ho_Chi_Minh")
      .startOf("day")
      .add(endHour, "hours")
      .add(endMinute, "minutes");
  } else {
    end = start.clone().add(30, "minutes");
  }

  return { start, end };
}

// TÌM NHÀ XE
async function findTransportCompany(
  address,
  dateDelivery,
  travelTime,
  orderId,
  preferredTransportName = "",
  timeHint = ""
) {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const { cleanedAddress, transportName } = preprocessAddress(address);
    const finalTransportName = preferredTransportName || transportName;

    if (!finalTransportName) {
      await connection.end();
      console.log(
        `[findTransportCompany] Không có nhà xe trong địa chỉ: ${address}`
      );
      return { remainingAddress: cleanedAddress };
    }

    const normalizedAddress = normalizeTransportName(finalTransportName);
    const [rows] = await connection.execute(
      `
      SELECT standardized_address, district, ward, source, departure_time, status
      FROM transport_companies
      WHERE UPPER(name) LIKE ?
      `,
      [`%${normalizedAddress}%`]
    );

    if (rows.length === 0) {
      console.log(
        `[findTransportCompany] Không tìm thấy nhà xe: ${finalTransportName}`
      );
      await connection.end();
      return { remainingAddress: cleanedAddress };
    }

    if (rows.length === 1) {
      console.log(
        `[findTransportCompany] Tìm thấy nhà xe: ${finalTransportName}, địa chỉ: ${rows[0].standardized_address}`
      );
      await connection.end();
      return {
        DcGiaohang: rows[0].standardized_address,
        District: rows[0].district,
        Ward: rows[0].ward,
        Source: "TransportDB",
      };
    }

    let selectedRow = null;
    if (dateDelivery && travelTime !== null) {
      let deliveryMoment;
      const dateMatch = dateDelivery.match(/(\d{2})\/(\d{2})\/(\d{4})/);
      if (dateMatch) {
        deliveryMoment = moment({
          year: parseInt(dateMatch[3]),
          month: parseInt(dateMatch[2]) - 1,
          day: parseInt(dateMatch[1]),
          hour: 0,
          minute: 0,
          second: 0,
        });
      } else {
        const [order] = await connection.execute(
          `SELECT created_at FROM orders WHERE id_order = ?`,
          [orderId]
        );
        deliveryMoment = moment(order[0]?.created_at || new Date());
      }

      if (!deliveryMoment.isValid()) {
        console.warn(
          `[findTransportCompany] date_delivery/created_at không hợp lệ cho ${orderId}`
        );
      } else {
        const estimatedTime = deliveryMoment.add(15 + travelTime, "minutes");
        let timeMatched = false;

        if (timeHint) {
          for (const row of rows) {
            const { start, end } = parseDepartureTime(row.departure_time);
            if (start && end) {
              const startHour = start.hour();
              const endHour = end.hour();
              if (
                (timeHint === "sáng" && startHour >= 0 && endHour <= 12) ||
                (timeHint === "chiều" && startHour >= 12 && endHour <= 18) ||
                (timeHint === "tối" && startHour >= 18)
              ) {
                if (estimatedTime.isBetween(start, end, null, "[]")) {
                  selectedRow = row;
                  timeMatched = true;
                  break;
                }
              }
            }
          }
        }

        if (!timeMatched) {
          for (const row of rows) {
            const { start, end } = parseDepartureTime(row.departure_time);
            if (
              start &&
              end &&
              estimatedTime.isBetween(start, end, null, "[]")
            ) {
              selectedRow = row;
              break;
            }
          }
        }
      }
    }

    if (!selectedRow) {
      selectedRow = rows[0];
      console.warn(
        `[findTransportCompany] Nhiều nhà xe trùng tên: ${finalTransportName}, chọn mặc định: ${selectedRow.standardized_address}`
      );
    }

    console.log(
      `[findTransportCompany] Tìm thấy nhà xe: ${finalTransportName}, địa chỉ: ${selectedRow.standardized_address}`
    );
    await connection.end();
    return {
      DcGiaohang: selectedRow.standardized_address,
      District: selectedRow.district,
      Ward: selectedRow.ward,
      Source: "TransportDB",
    };
  } catch (error) {
    console.error("[findTransportCompany] Lỗi:", error.message);
    return { remainingAddress: address };
  }
}

// PHÂN TÍCH THỜI GIAN KHỞI HÀNH NHÀ XE
function parseDeliveryNoteForAddress(note) {
  if (!note)
    return {
      transportName: "",
      address: "",
      timeHint: "",
      priority: 0,
      deliveryDate: "",
      cargoType: "",
    };

  const normalizedNote = note
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

  const transportMatch = normalizedNote.match(
    /(?:nhà xe|xe|chành xe|gửi xe)\s*[:\-]?\s*([\w\s]+?)(?=\s*(?:giao ở|giao tại|địa chỉ|giao đến|sáng|chiều|tối|hôm nay|ngày mai|ngày mốt|thứ [a-z]+|$))/i
  );
  let transportName = "";
  if (transportMatch) {
    transportName = transportMatch[1].trim();
  }

  let address = "";
  const addressMatch = normalizedNote.match(
    /(?:giao ở|giao tại|địa chỉ|giao đến|địa chỉ giao hàng|giao)\s*[:\-]?\s*(\d+\s+[^\d,]+(?:,\s*[^\d,]+)*)(?=\s*(?:sáng|chiều|tối|gấp|nhanh|hôm nay|ngày mai|ngày mốt|thứ [a-z]+|$))/i
  );
  if (addressMatch) {
    address = addressMatch[1].trim();
  } else {
    const potentialAddress = normalizedNote
      .replace(/(?:xe|nhà xe|chành xe|gửi xe)\s*[:\-]?\s*[\w\s]+/i, "")
      .replace(
        /(?:giao vào|giao trước|giao gấp|giao nhanh|hàng dễ vỡ|hàng nặng|hàng gấp|hàng lạnh|hàng tươi|sáng|chiều|tối|hôm nay|ngày mai|ngày mốt|thứ [a-z]+)\s*[:\-]?\s*[\w\s]+/gi,
        ""
      )
      .trim();
    if (potentialAddress.match(/\d+\s+[^\d\s]+/i)) {
      address = potentialAddress;
    }
  }

  const timeHintMatch = normalizedNote.match(
    /(?:giao vào|giao trước)\s*[:\-]?\s*(sáng|chiều|tối|\d{1,2}(?::\d{2})?(?:h|am|pm)?)(?=\s|$)/i
  );
  const timeHint = timeHintMatch
    ? timeHintMatch[1]
    : normalizedNote.match(/\b(sáng|chiều|tối)\b/i)?.[0] || "";

  const priorityMatch = normalizedNote.match(
    /\b(gấp|nhanh|sớm|khẩn cấp|hỏa tốc|mau lên)\b/i
  );
  const priority = priorityMatch ? 1 : 0;

  const deliveryDateMatch = normalizedNote.match(
    /\b(hôm nay|ngày mai|ngày mốt|thứ hai|thứ ba|thứ tư|thứ năm|thứ sáu|thứ bảy|chủ nhật)\b/i
  );
  const deliveryDate = deliveryDateMatch ? deliveryDateMatch[0] : "";

  const cargoTypeMatch = normalizedNote.match(
    /\b(hàng dễ vỡ|hàng nặng|hàng gấp|hàng lạnh|hàng tươi)\b/i
  );
  const cargoType = cargoTypeMatch ? cargoTypeMatch[0] : "";

  return {
    transportName,
    address,
    timeHint,
    priority,
    deliveryDate,
    cargoType,
  };
}

// =========================================================== REGEX ĐỊA CHỈ GIAO HÀNG =========================================================
// CHECK NẾU LÀ ĐỊA CHỈ NHÀ XE
function isTransportAddress(address) {
  if (!address) return false;
  const lowerAddress = address.toUpperCase();
  return TRANSPORT_KEYWORDS.some((keyword) => lowerAddress.includes(keyword));
}

// CHUẨN HÓA TÊN NHÀ XE
function normalizeTransportName(name) {
  if (!name) return "";
  let normalized = name
    .toUpperCase()
    .replace(/\b\d{10,11}\b/g, "")
    .replace(/^(GỬI\s+)?(XE|CHÀNH\s+XE|NHÀ\s+XE|XE\s+KHÁCH)\s+/i, "")
    .replace(/\s+/g, " ")
    .trim()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  return normalized;
}

// KIỂM TRA ĐỊA CHỈ HỢP LỆ
function isValidAddress(address) {
  if (!address || address.trim() === "") return false;
  return true;
}

// LẤY DANH SÁCH ID ĐƠN HÀNG HỢP LỆ
async function getValidOrderIds() {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);
    const [rows] = await connection.execute("SELECT id_order FROM orders");
    await connection.end();
    console.log(`getValidOrderIds thực thi trong ${Date.now() - startTime}ms`);
    return new Set(rows.map((row) => row.id_order));
  } catch (error) {
    console.error("Lỗi khi lấy danh sách id_order:", error.message);
    return new Set();
  }
}

// CHUẨN HÓA ĐỊA CHỈ ĐỂ LƯU CACHE
function normalizeForCache(address) {
  if (!address) return "";
  return address
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s-/]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

// LÀM SẠCH ĐỊA CHỈ
function cleanAddress(address) {
  if (!address) return "";

  return address
    .replace(/\b\d{10,11}\b/g, "")
    .replace(/\b(Anh|Chị|Ms\.|Mr\.|Người nhận)\s+[^\s,.;:()]+/gi, "")
    .replace(/\s*,\s*/g, ", ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractTransportInfo(address) {
  if (!address) return { transportName: "", transportAddress: "" };

  // Regex để tách tên nhà xe và địa chỉ nhà xe (trong ngoặc hoặc sau tên nhà xe)
  const transportMatch = address.match(
    /^(Nhà xe|Xe|Chành xe|Gửi xe)\s*[:\-]?\s*([^,;\-\/]+?)(?:\s*\(([^)]+)\)|\s*(?:,|;|\/\/|\-|\/|$))?/i
  );

  if (transportMatch) {
    return {
      transportName: transportMatch[2].trim(),
      transportAddress: transportMatch[3] ? transportMatch[3].trim() : "",
    };
  }

  return { transportName: "", transportAddress: "" };
}

// TÁCH ĐỊA CHỈ GIAO HÀNG
function handleDeliveryOnlyAddress(address) {
  if (!address) return { specificAddress: "", cleanedAddress: "" };

  // Danh sách từ khóa giao hàng
  const deliveryKeywords = [
    "GỬI VỀ",
    "GỬI ĐẾN",
    "GIAO Ở",
    "GIAO TẠI",
    "GIAO ĐẾN",
  ];

  // Tách địa chỉ sau từ khóa giao hàng
  let specificAddress = address;
  for (const keyword of deliveryKeywords) {
    const regex = new RegExp(`^${keyword}\\s*(.*)$`, "i");
    const match = address.match(regex);
    if (match) {
      specificAddress = match[1].trim();
      break;
    }
  }

  // Làm sạch địa chỉ
  const cleanedAddress = cleanAddress(specificAddress);

  console.log(`[handleDeliveryOnlyAddress] Đầu vào: ${address}, Kết quả:`, {
    specificAddress,
    cleanedAddress,
  });
  return { specificAddress, cleanedAddress };
}

// TÁCH ĐỊA CHỈ NHÀ XE
function handleTransportOnlyAddress(address) {
  if (!address)
    return { transportName: "", specificAddress: "", cleanedAddress: "" };

  const { transportName, transportAddress } = extractTransportInfo(address);

  if (!transportName) {
    return { transportName: "", specificAddress: "", cleanedAddress: "" };
  }

  const specificAddress = transportAddress || "";
  const cleanedAddress = cleanAddress(specificAddress || address);

  console.log(`[handleTransportOnlyAddress] Đầu vào: ${address}, Kết quả:`, {
    transportName,
    specificAddress,
    cleanedAddress,
  });
  return { transportName, specificAddress, cleanedAddress };
}

// TÁCH ĐỊA CHỈ NHÀ XE VỚI ĐỊA CHỈ KHÔNG RÕ RÀNG
function handleTransportWithUnknownAddress(address) {
  if (!address)
    return { transportName: "", specificAddress: "", cleanedAddress: "" };

  const { transportName, transportAddress } = extractTransportInfo(address);

  if (!transportName || !transportAddress) {
    return { transportName: "", specificAddress: "", cleanedAddress: "" };
  }

  const specificAddress = transportAddress;
  const cleanedAddress = cleanAddress(specificAddress);

  console.log(
    `[handleTransportWithUnknownAddress] Đầu vào: ${address}, Kết quả:`,
    {
      transportName,
      specificAddress,
      cleanedAddress,
    }
  );
  return { transportName, specificAddress, cleanedAddress };
}

// TÁCH ĐỊA CHỈ NHÀ XE VỚI ĐỊA CHỈ GIAO HÀNG
function handleTransportWithDeliveryAddress(address) {
  if (!address)
    return { transportName: "", specificAddress: "", cleanedAddress: "" };

  // Tách thông tin nhà xe
  const { transportName, transportAddress } = extractTransportInfo(address);

  if (!transportName) {
    return { transportName: "", specificAddress: "", cleanedAddress: "" };
  }

  // Danh sách từ khóa giao hàng
  const deliveryKeywords = [
    "GỬI VỀ",
    "GỬI ĐẾN",
    "GIAO Ở",
    "GIAO TẠI",
    "GIAO ĐẾN",
  ];
  let hasDeliveryAddress = false;
  for (const keyword of deliveryKeywords) {
    const regex = new RegExp(`${keyword}\\s*(.*)$`, "i");
    if (address.match(regex)) {
      hasDeliveryAddress = true;
      break;
    }
  }

  // Ưu tiên địa chỉ nhà xe làm specificAddress
  const specificAddress = transportAddress || "";
  const cleanedAddress = cleanAddress(specificAddress || address);

  console.log(
    `[handleTransportWithDeliveryAddress] Đầu vào: ${address}, Kết quả:`,
    {
      transportName,
      specificAddress,
      cleanedAddress,
      hasDeliveryAddress,
    }
  );
  return { transportName, specificAddress, cleanedAddress };
}

// TÁCH ĐỊA CHỈ KHÁC
function handleOtherCases(address) {
  if (!address) return { specificAddress: "", cleanedAddress: "" };

  const specificAddress = address;
  const cleanedAddress = cleanAddress(specificAddress);

  console.log(`[handleOtherCases] Đầu vào: ${address}, Kết quả:`, {
    specificAddress,
    cleanedAddress,
  });
  return { specificAddress, cleanedAddress };
}

// CHUẨN HÓA ĐỊA CHỈ
function preprocessAddress(address) {
  if (!address)
    return { cleanedAddress: "", transportName: "", specificAddress: "" };

  // Tách thông tin nhà xe
  const { transportName, transportAddress } = extractTransportInfo(address);

  if (transportName) {
    // Nếu có nhà xe
    if (transportAddress) {
      // Có địa chỉ nhà xe
      // Kiểm tra từ khóa giao hàng để xác định có địa chỉ cần giao hay không
      const deliveryKeywords = [
        "GỬI VỀ",
        "GỬI ĐẾN",
        "GIAO Ở",
        "GIAO TẠI",
        "GIAO ĐẾN",
      ];
      let hasDeliveryAddress = false;
      for (const keyword of deliveryKeywords) {
        const regex = new RegExp(`${keyword}\\s*(.*)$`, "i");
        if (address.match(regex)) {
          hasDeliveryAddress = true;
          break;
        }
      }
      if (hasDeliveryAddress) {
        // Có địa chỉ cần giao đến
        return handleTransportWithDeliveryAddress(address);
      } else {
        // Chỉ có địa chỉ nhà xe
        return handleTransportWithUnknownAddress(address);
      }
    } else {
      // Chỉ có tên nhà xe
      return handleTransportOnlyAddress(address);
    }
  } else {
    // Không có nhà xe
    if (address.match(/\d+\s+[^\d\s]+/i)) {
      // Có địa chỉ cần giao (sau từ khóa hoặc địa chỉ cụ thể)
      return handleDeliveryOnlyAddress(address);
    } else {
      // Các trường hợp bất thường
      return handleOtherCases(address);
    }
  }
}

// ========================================================= CACHING =========================================================
// CHECK CACHE
async function checkRouteCache(cleanedAddress, originalAddress) {
  try {
    const normalizedAddress = normalizeForCache(cleanedAddress);
    const connection = await mysql.createConnection(dbConfig);
    const [rows] = await connection.query(
      `SELECT standardized_address, district, ward, distance, travel_time
       FROM route_cache
       WHERE normalized_address = ?`,
      [normalizedAddress]
    );
    await connection.end();
    if (rows.length > 0) {
      console.log(`[checkRouteCache] Cache hit cho địa chỉ: ${cleanedAddress}`);
      return rows[0];
    }
    console.log(
      `[checkRouteCache] Không tìm thấy cache cho địa chỉ: ${cleanedAddress}`
    );
    return null;
  } catch (error) {
    console.error("[checkRouteCache] Lỗi:", error.message);
    return null;
  }
}

// LƯU CACHE
async function saveRouteToCache(
  originalAddress,
  standardizedAddress,
  district,
  ward,
  distance,
  travel_time
) {
  try {
    const normalizedAddress = normalizeForCache(standardizedAddress);
    const connection = await mysql.createConnection(dbConfig);
    await connection.query(
      `INSERT INTO route_cache (original_address, normalized_address, standardized_address, district, ward, distance, travel_time)
       VALUES (?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         normalized_address = VALUES(normalized_address),
         standardized_address = VALUES(standardized_address),
         district = VALUES(district),
         ward = VALUES(ward),
         distance = VALUES(distance),
         travel_time = VALUES(travel_time)`,
      [
        originalAddress,
        normalizedAddress,
        standardizedAddress,
        district,
        ward,
        distance,
        travel_time,
      ]
    );
    await connection.end();
    console.log(
      `[saveRouteToCache] Lưu cache thành công cho địa chỉ: ${originalAddress}`
    );
  } catch (error) {
    console.error("[saveRouteToCache] Lỗi:", error.message);
    throw error;
  }
}

// ========================================================= TOMTOM API =========================================================
// GỌI TOMTOM GEOCODE API
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

// TÍNH TOÁN ĐƯỜNG ĐI
async function calculateRoute(
  destinationAddress,
  originalAddress,
  district,
  ward
) {
  const startTime = Date.now();

  // Kiểm tra giá trị của WAREHOUSE_ADDRESS
  if (
    !WAREHOUSE_ADDRESS ||
    typeof WAREHOUSE_ADDRESS !== "string" ||
    WAREHOUSE_ADDRESS.trim() === ""
  ) {
    throw new Error(
      "WAREHOUSE_ADDRESS không được định nghĩa hoặc không hợp lệ trong biến môi trường."
    );
  }

  const originAddress = WAREHOUSE_ADDRESS;

  const cacheResult = await checkRouteCache(
    destinationAddress,
    originalAddress
  );
  if (cacheResult) {
    console.log(
      `[calculateRoute] Sử dụng cache cho địa chỉ: ${destinationAddress}`
    );
    return cacheResult;
  }

  const run = async () => {
    const origin = await geocodeAddress(originAddress);
    const destination = await geocodeAddress(destinationAddress);

    if (!origin || !destination) {
      console.warn(
        `[calculateRoute] Không thể lấy tọa độ cho địa chỉ: ${destinationAddress}`
      );
      return { distance: null, travel_time: null };
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
      const travel_time = Math.ceil(route.summary.travelTimeInSeconds / 60);
      return { distance, travel_time };
    }
    return { distance: null, travel_time: null };
  };

  try {
    const result = await retry(run);
    if (result.distance !== null && result.travel_time !== null) {
      await saveRouteToCache(
        originalAddress,
        destinationAddress,
        district,
        ward,
        result.distance,
        result.travel_time
      );
    }
    console.log(`[calculateRoute] Thực thi trong ${Date.now() - startTime}ms`);
    return result;
  } catch (error) {
    console.error(
      `[calculateRoute] Lỗi khi gọi TomTom Routing API đến ${destinationAddress}:`,
      error.message
    );
    return { distance: null, travel_time: null };
  }
}

// TÍNH TOÁN KHOẢNG CÁCH
async function calculateDistances() {
  const startTime = Date.now();
  let tomtomCalls = 0;
  try {
    const connection = await mysql.createConnection(dbConfig);
    const [orders] = await connection.query(
      `
      SELECT oa.id_order, oa.address, o.address AS original_address, oa.district, oa.ward, oa.source
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.address IS NOT NULL AND oa.address != ''
        AND (oa.distance IS NULL OR oa.travel_time IS NULL)
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
      `
    );

    if (orders.length === 0) {
      console.log(
        "[calculateDistances] Không có đơn hàng mới hoặc cần tính lại khoảng cách, bỏ qua."
      );
      await connection.end();
      console.log(
        `[calculateDistances] Thực thi trong ${
          Date.now() - startTime
        }ms, TomTom calls: ${tomtomCalls}`
      );
      return;
    }

    console.log(
      `[calculateDistances] Các đơn hàng để tính khoảng cách: ${orders.length}`
    );

    const addressMap = {};
    const expressDeliveryOrders = [];

    orders.forEach((order) => {
      if (order.address.toUpperCase().includes("CHUYỂN PHÁT NHANH")) {
        expressDeliveryOrders.push(order.id_order);
      } else {
        if (!addressMap[order.address]) {
          addressMap[order.address] = [];
        }
        addressMap[order.address].push({
          id_order: order.id_order,
          original_address: order.original_address,
          district: order.district,
          ward: order.ward,
          source: order.source,
        });
      }
    });

    const uniqueAddresses = Object.keys(addressMap);
    const limit = pLimit(2);
    const routePromises = uniqueAddresses.map((address) =>
      limit(async () => {
        console.log(
          `[calculateDistances] Tính tuyến đường cho địa chỉ: ${address}`
        );
        const orderInfo = addressMap[address][0];
        if (orderInfo.source === "Original") {
          console.log(
            `[calculateDistances] Địa chỉ chưa chuẩn hóa, thử gọi TomTom API: ${address}`
          );
        }
        const route = await calculateRoute(
          address,
          orderInfo.original_address,
          orderInfo.district,
          orderInfo.ward
        );
        tomtomCalls++;
        if (route.distance === null || route.travel_time === null) {
          return { address, distance: 0, travel_time: 0 };
        }
        // Lưu vào route_cache với distance và travel_time
        await saveRouteToCache(
          orderInfo.original_address,
          address,
          orderInfo.district,
          orderInfo.ward,
          route.distance,
          route.travel_time
        );
        return { address, ...route };
      })
    );

    const routeResults = await Promise.all(routePromises);
    console.log(`[calculateDistances] Kết quả tính khoảng cách:`, routeResults);

    const updateValues = [];
    routeResults.forEach((result) => {
      const { address, distance, travel_time } = result;
      addressMap[address].forEach(({ id_order }) => {
        updateValues.push([id_order, distance, travel_time]);
      });
    });

    expressDeliveryOrders.forEach((id_order) => {
      updateValues.push([id_order, null, null]);
    });

    if (updateValues.length > 0) {
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
        "[calculateDistances] Số dòng ảnh hưởng khi cập nhật khoảng cách và thời gian:",
        updateResult.affectedRows
      );
    }

    await connection.end();
    console.log(
      `[calculateDistances] Thực thi trong ${
        Date.now() - startTime
      }ms, TomTom calls: ${tomtomCalls}`
    );
  } catch (error) {
    console.error("[calculateDistances] Lỗi:", error.message);
    throw error;
  }
}

// ========================================================= CRON JOB =========================================================
// LẤY ĐƠN HÀNG TỪ API_1 VÀ LƯU VÀO CSDL
async function fetchAndSaveOrders() {
  const startTime = Date.now();
  let api2RequestCount = 0;
  try {
    console.log("📦 [fetchAndSaveOrders] Bắt đầu lấy dữ liệu từ API_1...");
    const response1 = await retry(() => axios.get(API_1));
    const orders = response1.data;

    console.log(`Có ${orders.length} đơn hàng từ API_1`);

    // Tạo hash để so sánh dữ liệu, bao gồm cả DiachiTruSo
    const currentHash = orders
      .map((o) => `${o.MaPX}:${o.DcGiaohang}:${o.DiachiTruSo}`)
      .sort()
      .join("|");
    if (currentHash === lastApiOrderCount && orders.length > 0) {
      console.log(
        "[fetchAndSaveOrders] Dữ liệu không thay đổi, bỏ qua gọi API_2."
      );
      console.log(
        `[fetchAndSaveOrders] Thực thi trong ${
          Date.now() - startTime
        }ms, API_2 calls: ${api2RequestCount}`
      );
      return [];
    }

    lastApiOrderCount = currentHash;

    const connection = await createConnectionWithRetry();
    const [existingOrders] = await connection.query(
      `SELECT id_order, address, old_address, DiachiTruSo FROM orders WHERE id_order IN (?)`,
      [orders.map((order) => order.MaPX)]
    );
    const addressMap = new Map(
      existingOrders.map((o) => [
        o.id_order,
        {
          address: o.address,
          old_address: o.old_address,
          DiachiTruSo: o.DiachiTruSo,
        },
      ])
    );

    const limit = pLimit(5);
    const api2Promises = orders.map((order) =>
      limit(async () => {
        try {
          const res = await retry(() =>
            axios.get(`${API_2_BASE}?qc=${order.MaPX}`)
          );
          api2RequestCount++;
          const currentAddress = addressMap.get(order.MaPX)?.address || "";
          const newAddress = res.data.DcGiaohang || "";
          const addressChanged =
            currentAddress && currentAddress !== newAddress;
          return {
            MaPX: order.MaPX,
            DcGiaohang: newAddress,
            Tinhtranggiao: res.data.Tinhtranggiao || "",
            SOKM: order.SOKM,
            GhiChu: order.GhiChu,
            Ngayxuatkho: order.Ngayxuatkho,
            NgayPX: order.NgayPX,
            DiachiTruSo: order.DiachiTruSo || "", // Lấy DiachiTruSo từ API_1
            isEmpty: !newAddress,
            addressChanged,
            old_address: addressChanged
              ? currentAddress
              : addressMap.get(order.MaPX)?.old_address || null,
          };
        } catch (err) {
          console.error(
            `[fetchAndSaveOrders] Lỗi khi gọi API_2 cho MaPX ${order.MaPX}:`,
            err.message
          );
          return null;
        }
      })
    );

    const settledResults = await Promise.allSettled(api2Promises);
    const results = settledResults
      .filter(
        (result) => result.status === "fulfilled" && result.value !== null
      )
      .map((result) => result.value);

    const pendingOrders = results.filter(
      (order) => order.Tinhtranggiao === "Chờ xác nhận giao/lấy hàng"
    );

    if (pendingOrders.length === 0) {
      await connection.end();
      console.log(
        `[fetchAndSaveOrders] Thực thi trong ${
          Date.now() - startTime
        }ms, API_2 calls: ${api2RequestCount}`
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
        order.GhiChu,
        order.Ngayxuatkho,
        ngayPX,
        order.old_address,
        order.DiachiTruSo, // Thêm DiachiTruSo vào giá trị lưu
      ];
    });

    const [insertResult] = await connection.query(
      `
      INSERT INTO orders (id_order, address, status, SOKM, delivery_note, date_delivery, created_at, old_address, DiachiTruSo)
      VALUES ?
      ON DUPLICATE KEY UPDATE
      address = IF(VALUES(address) != '', VALUES(address), address),
      status = VALUES(status),
      SOKM = VALUES(SOKM),
      delivery_note = VALUES(delivery_note),
      date_delivery = VALUES(date_delivery),
      created_at = VALUES(created_at),
      old_address = IF(VALUES(old_address) IS NOT NULL AND old_address IS NULL, VALUES(old_address), old_address),
      DiachiTruSo = VALUES(DiachiTruSo)
      `,
      [values]
    );

    await connection.end();
    console.log(
      `[fetchAndSaveOrders] Thực thi trong ${
        Date.now() - startTime
      }ms, API_2 calls: ${api2RequestCount}`
    );
    return pendingOrders;
  } catch (error) {
    console.error("[fetchAndSaveOrders] Lỗi:", error.message, error.stack);
    throw error;
  }
}

// ========================================================= PROMPT & OPEN AI =========================================================
// XÂY DỰNG PROMPT CHUẨN HÓA ĐỊA CHỈ
const buildPrompt = (maPX, address) => {
  const escapedAddress = address.replace(/"/g, '\\"');
  return `
Bạn là AI chuẩn hóa địa chỉ Việt Nam (2025). Nhiệm vụ là phân tích và chuẩn hóa địa chỉ trong trường "DcGiaohang" thành định dạng: "[Số nhà, Đường], [Phường/Xã], [Quận/Huyện/Thị xã/Thành phố], [Tỉnh/Thành phố], Việt Nam". Tách riêng District (Quận/Huyện) và Ward (Phường/Xã).

### Hướng dẫn:
1. Loại bỏ tên người, số điện thoại, chú thích không liên quan.
2. Chuẩn hóa:
   - Q.1 → Quận 1, P.12 → Phường 12, TP.Hà Nội → Thành phố Hà Nội.
3. Suy luận tỉnh/thành phố:
   - Quận 1, Tân Bình → Hồ Chí Minh.
   - TP Đà Nẵng → Đà Nẵng.
   - Nếu không suy luận được, hãy tìm kiếm thông tin từ các trang đáng tin cậy để lấy thông tin chính xác.
4. Kiểm tra hợp lệ:
   - Phường/Xã phải thuộc Quận/Huyện. Nếu không hợp lệ, tìm nguồn khác để sửa.
   - Ví dụ: "191 Bùi Thị Xuân, Quận Tân Bình" thuộc Phường 1, không phải Phường 6.
5. Ưu tiên địa chỉ cụ thể (số nhà, đường, phường, quận) dù có từ khóa nhà xe (XE, CHÀNH XE).
6. Nếu chỉ có tên nhà xe (ví dụ: "Gửi xe Kim Mã"), trả về null cho DcGiaohang, District, Ward.
7. Trả về **chỉ chuỗi JSON** dạng [{...}], không thêm văn bản, ký tự, hoặc định dạng nào khác.

### Ví dụ minh họa:
#### Ví dụ 1:
Đầu vào: [{"MaPX":"X241019078-N","DcGiaohang":"191 BÙI THỊ XUÂN, PHƯỜNG 6, QUẬN TÂN BÌNH"}]
Đầu ra: [{"MaPX":"X241019078-N","DcGiaohang":"191 Bùi Thị Xuân, Phường 1, Quận Tân Bình, Hồ Chí Minh, Việt Nam","District":"Quận Tân Bình","Ward":"Phường 1","Source":"OpenAI"}]

#### Ví dụ 2:
Đầu vào: [{"MaPX":"X2410190xx-N","DcGiaohang":"Gửi xe Kim Mã"}]
Đầu ra: [{"MaPX":"X2410190xx-N","DcGiaohang":null,"District":null,"Ward":null,"Source":null}]

### Nhiệm vụ:
Phân tích địa chỉ sau và trả về chuỗi JSON chuẩn hóa:
Đầu vào: [{"MaPX":"${maPX}","DcGiaohang":"${escapedAddress}"}]
Đầu ra: [{"MaPX":"${maPX}","DcGiaohang":"Địa chỉ chuẩn hóa hoặc null","District":"Quận/Huyện hoặc null","Ward":"Phường/Xã hoặc null","Source":"OpenAI hoặc null"}]
`.trim();
};

// Hàm hỗ trợ gọi OpenAI
async function callOpenAI(maPX, address) {
  const maxAttempts = 5;
  let attempt = 0;
  let openAIResult = null;

  const timeoutPromise = (promise, ms) => {
    const timeout = new Promise((_, reject) =>
      setTimeout(() => reject(new Error("Request timed out")), ms)
    );
    return Promise.race([promise, timeout]);
  };

  while (attempt < maxAttempts) {
    try {
      const prompt = buildPrompt(maPX, address);

      const response = await timeoutPromise(
        openai.chat.completions.create({
          model: "gpt-4o-mini-2024-07-18",
          messages: [{ role: "system", content: prompt }],
        }),
        20000
      );

      const content = response.choices[0]?.message?.content?.trim();

      let jsonContent = content;
      const jsonMatch = content.match(/```json\s*([\s\S]*?)\s*```/);
      if (jsonMatch && jsonMatch[1]) {
        jsonContent = jsonMatch[1].trim();
      }

      // Loại bỏ dấu \ (khôi phục đoạn mã)
      jsonContent = jsonContent.replace(/\\/g, "");

      let result;
      try {
        result = JSON.parse(jsonContent);
      } catch (parseErr) {
        console.warn(`[callOpenAI] ❌ Lỗi parse JSON: ${parseErr.message}`);
        throw new Error(`Không thể parse JSON từ nội dung: ${jsonContent}`);
      }

      if (
        Array.isArray(result) &&
        result[0] &&
        "DcGiaohang" in result[0] &&
        "District" in result[0] &&
        "Ward" in result[0]
      ) {
        openAIResult = { ...result[0], Source: "OpenAI" };
        break;
      } else {
        console.warn(
          `[callOpenAI] ⚠️ JSON không đúng định dạng yêu cầu:\n${JSON.stringify(
            result,
            null,
            2
          )}`
        );
        throw new Error("Kết quả JSON không hợp lệ");
      }
    } catch (err) {
      attempt++;
      console.warn(
        `[callOpenAI] Lỗi trong lần thử ${attempt} cho MaPX ${maPX}: ${err.message}`
      );

      if (err.message.includes("Request timed out")) {
        console.warn(`[callOpenAI] ⚠️ Timeout sau 20 giây, thử lại...`);
      }

      if (attempt >= maxAttempts) {
        console.warn(`[callOpenAI] ❌ Thất bại sau ${maxAttempts} lần thử.`);
        break;
      }

      await new Promise((res) => setTimeout(res, 5000 * attempt));
    }
  }

  if (!openAIResult) {
    console.warn(
      `[callOpenAI] ⛔ Không thể chuẩn hóa địa chỉ cho MaPX ${maPX}, trả về mặc định.`
    );
    return {
      MaPX: maPX,
      DcGiaohang: address,
      District: null,
      Ward: null,
      Source: "OpenAIFailed",
    };
  }

  return openAIResult;
}

// ========================================================= CHUẨN HÓA ĐỊA CHỈ =========================================================
// CHUẨN HÓA ĐỊA CHỈ BẰNG OPENAI
async function standardizeAddresses(orders) {
  const startTime = Date.now();
  let openAICalls = 0;
  try {
    const limit = pLimit(2);
    if (!orders || !Array.isArray(orders) || orders.length === 0) {
      console.log(
        "[standardizeAddresses] Không có đơn hàng nào để xử lý, orders: []"
      );
      return [];
    }

    console.log(
      `[standardizeAddresses] Bắt đầu xử lý ${orders.length} đơn hàng`
    );

    const orderIds = orders.map((order) => order.MaPX).filter(Boolean);
    if (orderIds.length === 0) {
      console.log("[standardizeAddresses] Không có MaPX hợp lệ để chuẩn hóa");
      return [];
    }

    const connection = await createConnectionWithRetry();
    console.log("[standardizeAddresses] Kết nối cơ sở dữ liệu thành công");

    const [existingAddresses] = await connection.query(
      `SELECT id_order, address, district, ward, source FROM orders_address
       WHERE id_order IN (${orderIds.map(() => "?").join(",")})`,
      orderIds
    );
    const [orderDetails] = await connection.query(
      `SELECT o.id_order, o.date_delivery, oa.travel_time, o.address AS current_address, o.delivery_note, o.SOKM, o.DiachiTruSo
       FROM orders o
       LEFT JOIN orders_address oa ON o.id_order = oa.id_order
       WHERE o.id_order IN (${orderIds.map(() => "?").join(",")})`,
      orderIds
    );
    await connection.end();

    console.log(
      `[standardizeAddresses] Lấy được ${existingAddresses.length} địa chỉ hiện có`
    );
    console.log(
      `[standardizeAddresses] Lấy được ${orderDetails.length} chi tiết đơn hàng`
    );

    const addressMap = new Map(
      existingAddresses.map((row) => [row.id_order, row])
    );
    const orderDetailMap = new Map(
      orderDetails.map((row) => [row.id_order, row])
    );
    const results = [];
    let transportResults = [];

    const batchSize = 50;
    for (let i = 0; i < orders.length; i += batchSize) {
      console.log(
        `[standardizeAddresses] Xử lý batch từ ${i} đến ${Math.min(
          i + batchSize,
          orders.length
        )}`
      );
      const batch = orders.slice(i, i + batchSize);
      const batchResults = await Promise.all(
        batch.map((order) =>
          limit(async () => {
            const orderStartTime = Date.now();
            const MaPX = order.MaPX;
            const DcGiaohang = order.DcGiaohang;
            const existingAddress = addressMap.get(MaPX);
            const orderDetail = orderDetailMap.get(MaPX);

            console.log(
              `[standardizeAddresses] Bắt đầu xử lý MaPX ${MaPX}: DcGiaohang=${JSON.stringify(
                DcGiaohang
              )}, DiachiTruSo=${JSON.stringify(
                orderDetail?.DiachiTruSo
              )}, existingAddress=${JSON.stringify(existingAddress)}`
            );

            if (
              existingAddress &&
              existingAddress.address &&
              existingAddress.district &&
              existingAddress.ward &&
              orderDetail?.current_address === DcGiaohang
            ) {
              console.log(
                `[standardizeAddresses] Bỏ qua MaPX ${MaPX}: Địa chỉ đã chuẩn hóa`
              );
              return { ...existingAddress, MaPX, isEmpty: false };
            }

            // Sử dụng DiachiTruSo nếu DcGiaohang rỗng
            const addressToProcess = !DcGiaohang
              ? orderDetail?.DiachiTruSo || ""
              : DcGiaohang;

            if (!addressToProcess) {
              console.log(
                `[standardizeAddresses] Cả DcGiaohang và DiachiTruSo đều rỗng cho MaPX ${MaPX}`
              );
              const SOKM =
                orderDetail?.SOKM && !isNaN(parseFloat(orderDetail.SOKM))
                  ? parseFloat(orderDetail.SOKM)
                  : null;
              const travelTime = SOKM
                ? getTravelTimeByTimeFrame(SOKM, orderDetail?.date_delivery)
                : 0;
              const result = {
                MaPX,
                DcGiaohang: "",
                District: null,
                Ward: null,
                Source: "Empty",
                isEmpty: true,
                distance: SOKM || 0,
                travel_time: travelTime,
                priority: order.priority || 0,
                deliveryDate: order.deliveryDate || "",
                cargoType: order.cargoType || "",
              };
              console.log(
                `[standardizeAddresses] Kết quả cho MaPX ${MaPX} (rỗng): ${JSON.stringify(
                  result
                )}, thời gian: ${Date.now() - orderStartTime}ms`
              );
              return result;
            }

            if (!isValidAddress(addressToProcess)) {
              console.log(
                `[standardizeAddresses] Địa chỉ không hợp lệ cho MaPX ${MaPX}: ${addressToProcess}`
              );
              const SOKM =
                orderDetail?.SOKM && !isNaN(parseFloat(orderDetail.SOKM))
                  ? parseFloat(orderDetail.SOKM)
                  : null;
              const travelTime = SOKM
                ? getTravelTimeByTimeFrame(SOKM, orderDetail?.date_delivery)
                : 0;
              const result = {
                MaPX,
                DcGiaohang: addressToProcess,
                District: null,
                Ward: null,
                Source: "Invalid",
                isEmpty: true,
                distance: SOKM || 0,
                travel_time: travelTime,
                priority: order.priority || 0,
                deliveryDate: order.deliveryDate || "",
                cargoType: order.cargoType || "",
              };
              console.log(
                `[standardizeAddresses] Kết quả cho MaPX ${MaPX} (không hợp lệ): ${JSON.stringify(
                  result
                )}, thời gian: ${Date.now() - orderStartTime}ms`
              );
              return result;
            }

            const expressKeywords = ["chuyển phát nhanh", "cpn"];
            const isExpressDelivery = expressKeywords.some((keyword) =>
              addressToProcess.toLowerCase().includes(keyword)
            );
            if (isExpressDelivery) {
              console.log(
                `[standardizeAddresses] Địa chỉ chứa "chuyển phát nhanh" cho MaPX ${MaPX}: ${addressToProcess}`
              );
              const result = {
                MaPX,
                DcGiaohang: addressToProcess,
                District: null,
                Ward: null,
                Source: "Express",
                isEmpty: false,
                distance: 0,
                travel_time: 0,
                priority: order.priority || 0,
                deliveryDate: order.deliveryDate || "",
                cargoType: order.cargoType || "",
              };
              console.log(
                `[standardizeAddresses] Kết quả cho MaPX ${MaPX} (chuyển phát nhanh): ${JSON.stringify(
                  result
                )}, thời gian: ${Date.now() - orderStartTime}ms`
              );
              return result;
            }

            const noteInfo = parseDeliveryNoteForAddress(
              orderDetail?.delivery_note
            );
            console.log(
              `[standardizeAddresses] Ghi chú giao hàng cho MaPX ${MaPX}: ${JSON.stringify(
                noteInfo
              )}`
            );

            const isTransport = isTransportAddress(addressToProcess);
            const { cleanedAddress, transportName, specificAddress } =
              preprocessAddress(addressToProcess);
            console.log(
              `[standardizeAddresses] Kết quả preprocessAddress cho MaPX ${MaPX}: cleanedAddress=${JSON.stringify(
                cleanedAddress
              )}, transportName=${transportName}, specificAddress=${JSON.stringify(
                specificAddress
              )}`
            );

            // Kiểm tra cache
            const cacheResult = await checkRouteCache(
              cleanedAddress,
              addressToProcess
            );
            if (cacheResult) {
              console.log(
                `[standardizeAddresses] Sử dụng cache cho MaPX ${MaPX}: ${JSON.stringify(
                  cacheResult
                )}`
              );
              const result = {
                MaPX,
                DcGiaohang: cacheResult.standardized_address,
                District: cacheResult.district,
                Ward: cacheResult.ward,
                Source: "RouteCache",
                isEmpty: false,
                distance: cacheResult.distance,
                travel_time: cacheResult.travel_time,
                priority: noteInfo.priority || 0,
                deliveryDate: noteInfo.deliveryDate || "",
                cargoType: noteInfo.cargoType || "",
              };
              console.log(
                `[standardizeAddresses] Kết quả cho MaPX ${MaPX} (cache): ${JSON.stringify(
                  result
                )}, thời gian: ${Date.now() - orderStartTime}ms`
              );
              return result;
            }

            if (isTransport && transportName) {
              const transportResult = await findTransportCompany(
                addressToProcess,
                orderDetail?.date_delivery,
                orderDetail?.travel_time || 15,
                MaPX,
                transportName,
                noteInfo.timeHint || ""
              );
              if (transportResult.DcGiaohang) {
                console.log(
                  `[standardizeAddresses] Nhà xe tìm thấy cho MaPX ${MaPX}: ${JSON.stringify(
                    transportResult
                  )}`
                );
                const result = {
                  MaPX,
                  DcGiaohang: transportResult.DcGiaohang,
                  District: transportResult.District,
                  Ward: transportResult.Ward,
                  Source: "TransportDB",
                  isEmpty: false,
                  distance: null,
                  travel_time: null,
                  priority: noteInfo.priority || 0,
                  deliveryDate: noteInfo.deliveryDate || "",
                  cargoType: noteInfo.cargoType || "",
                };
                console.log(
                  `[standardizeAddresses] Kết quả cho MaPX ${MaPX} (transport): ${JSON.stringify(
                    result
                  )}, thời gian: ${Date.now() - orderStartTime}ms`
                );
                return result;
              }
              console.log(
                `[standardizeAddresses] Không tìm thấy nhà xe cho MaPX ${MaPX}`
              );
            }

            // Sử dụng specificAddress nếu có, nếu không dùng cleanedAddress
            const addressToStandardize =
              specificAddress || cleanedAddress || addressToProcess;
            console.log(
              `[standardizeAddresses] Địa chỉ gửi đến OpenAI cho MaPX ${MaPX}: ${JSON.stringify(
                addressToStandardize
              )}`
            );
            let openAIResult = await callOpenAI(MaPX, addressToStandardize);
            openAICalls++;

            if (
              openAIResult &&
              openAIResult.DcGiaohang &&
              openAIResult.District &&
              openAIResult.Ward
            ) {
              console.log(
                `[standardizeAddresses] OpenAI trả về cho MaPX ${MaPX}: ${JSON.stringify(
                  openAIResult
                )}`
              );
              // Lưu vào cache
              await saveRouteToCache(
                addressToProcess,
                openAIResult.DcGiaohang,
                openAIResult.District,
                openAIResult.Ward,
                null,
                null
              );
              console.log(
                `[standardizeAddresses] Lưu cache cho MaPX ${MaPX}: ${openAIResult.DcGiaohang}`
              );
              const result = {
                MaPX,
                DcGiaohang: openAIResult.DcGiaohang,
                District: openAIResult.District,
                Ward: openAIResult.Ward,
                Source: "OpenAI",
                isEmpty: false,
                distance: null,
                travel_time: null,
                priority: noteInfo.priority || 0,
                deliveryDate: noteInfo.deliveryDate || "",
                cargoType: noteInfo.cargoType || "",
              };
              console.log(
                `[standardizeAddresses] Kết quả cho MaPX ${MaPX} (OpenAI): ${JSON.stringify(
                  result
                )}, thời gian: ${Date.now() - orderStartTime}ms`
              );
              return result;
            }

            // OpenAI không chuẩn hóa được, thử DiachiTruSo
            console.log(
              `[standardizeAddresses] OpenAI không chuẩn hóa được cho MaPX ${MaPX}, kiểm tra DiachiTruSo`
            );
            if (
              orderDetail?.DiachiTruSo &&
              orderDetail.DiachiTruSo !== addressToProcess
            ) {
              const fallbackAddress = preprocessAddress(
                orderDetail.DiachiTruSo
              );
              console.log(
                `[standardizeAddresses] Kết quả preprocessAddress cho DiachiTruSo của MaPX ${MaPX}: ${JSON.stringify(
                  fallbackAddress
                )}`
              );

              const fallbackCache = await checkRouteCache(
                fallbackAddress.cleanedAddress,
                orderDetail.DiachiTruSo
              );
              if (fallbackCache) {
                console.log(
                  `[standardizeAddresses] Sử dụng cache cho DiachiTruSo của MaPX ${MaPX}: ${JSON.stringify(
                    fallbackCache
                  )}`
                );
                const result = {
                  MaPX,
                  DcGiaohang: fallbackCache.standardized_address,
                  District: fallbackCache.district,
                  Ward: fallbackCache.ward,
                  Source: "RouteCache",
                  isEmpty: false,
                  distance: fallbackCache.distance,
                  travel_time: fallbackCache.travel_time,
                  priority: noteInfo.priority || 0,
                  deliveryDate: noteInfo.deliveryDate || "",
                  cargoType: noteInfo.cargoType || "",
                };
                console.log(
                  `[standardizeAddresses] Kết quả cho MaPX ${MaPX} (cache DiachiTruSo): ${JSON.stringify(
                    result
                  )}, thời gian: ${Date.now() - orderStartTime}ms`
                );
                return result;
              }

              console.log(
                `[standardizeAddresses] Gọi OpenAI cho DiachiTruSo của MaPX ${MaPX}: ${JSON.stringify(
                  fallbackAddress.cleanedAddress
                )}`
              );
              openAIResult = await callOpenAI(
                MaPX,
                fallbackAddress.cleanedAddress
              );
              openAICalls++;

              if (
                openAIResult &&
                openAIResult.DcGiaohang &&
                openAIResult.District &&
                openAIResult.Ward
              ) {
                await saveRouteToCache(
                  orderDetail.DiachiTruSo,
                  openAIResult.DcGiaohang,
                  openAIResult.District,
                  openAIResult.Ward,
                  null,
                  null
                );
                console.log(
                  `[standardizeAddresses] Lưu cache cho DiachiTruSo của MaPX ${MaPX}: ${openAIResult.DcGiaohang}`
                );
                const result = {
                  MaPX,
                  DcGiaohang: openAIResult.DcGiaohang,
                  District: openAIResult.District,
                  Ward: openAIResult.Ward,
                  Source: "OpenAI",
                  isEmpty: false,
                  distance: null,
                  travel_time: null,
                  priority: noteInfo.priority || 0,
                  deliveryDate: noteInfo.deliveryDate || "",
                  cargoType: noteInfo.cargoType || "",
                };
                console.log(
                  `[standardizeAddresses] Kết quả cho MaPX ${MaPX} (OpenAI DiachiTruSo): ${JSON.stringify(
                    result
                  )}, thời gian: ${Date.now() - orderStartTime}ms`
                );
                return result;
              }
            }

            // Nếu cả OpenAI và DiachiTruSo thất bại, sử dụng địa chỉ gốc
            console.log(
              `[standardizeAddresses] Không chuẩn hóa được, dùng địa chỉ gốc: ${addressToProcess}`
            );
            const SOKM =
              orderDetail?.SOKM && !isNaN(parseFloat(orderDetail.SOKM))
                ? parseFloat(orderDetail.SOKM)
                : null;
            const travelTime = SOKM
              ? getTravelTimeByTimeFrame(SOKM, orderDetail?.date_delivery)
              : 0;
            const result = {
              MaPX,
              DcGiaohang: addressToProcess,
              District: null,
              Ward: null,
              Source: "Original",
              isEmpty: false,
              distance: SOKM || 0,
              travel_time: travelTime,
              priority: order.priority || 0,
              deliveryDate: order.deliveryDate || "",
              cargoType: order.cargoType || "",
            };
            console.log(
              `[standardizeAddresses] Kết quả cho MaPX ${MaPX} (gốc): ${JSON.stringify(
                result
              )}, thời gian: ${Date.now() - orderStartTime}ms`
            );
            return result;
          })
        )
      );
      results.push(...batchResults);
    }

    const validOrderIds = await getValidOrderIds();
    console.log(
      `[standardizeAddresses] Lấy được ${validOrderIds.size} MaPX hợp lệ`
    );

    const validResults = results.filter((order) =>
      validOrderIds.has(order.MaPX)
    );

    console.log(
      `[standardizeAddresses] Kết quả chuẩn hóa (${
        validResults.length
      } đơn): ${JSON.stringify(
        validResults.map((order) => ({
          MaPX: order.MaPX,
          DcGiaohang: order.DcGiaohang,
          District: order.District,
          Ward: order.Ward,
          Source: order.Source,
          distance: order.distance,
          travel_time: order.travel_time,
          priority: order.priority,
          deliveryDate: order.deliveryDate,
          cargoType: order.cargoType,
        }))
      )}`
    );

    if (validResults.length > 0) {
      const connection = await createConnectionWithRetry();
      const values = validResults
        .filter((order) => order.DcGiaohang !== undefined)
        .map((order) => [
          order.MaPX,
          order.DcGiaohang || "",
          order.District,
          order.Ward,
          order.Source,
          order.distance,
          order.travel_time,
        ]);
      if (values.length > 0) {
        const [insertResult] = await connection.query(
          `INSERT INTO orders_address (id_order, address, district, ward, source, distance, travel_time)
           VALUES ?
           ON DUPLICATE KEY UPDATE
             address = IF(VALUES(address) != '', VALUES(address), address),
             district = IF(VALUES(district) IS NOT NULL, VALUES(district), district),
             ward = IF(VALUES(ward) IS NOT NULL, VALUES(ward), ward),
             source = IF(VALUES(source) IS NOT NULL, VALUES(source), source),
             distance = VALUES(distance),
             travel_time = VALUES(travel_time)`,
          [values]
        );
        console.log(
          `[standardizeAddresses] Lưu ${insertResult.affectedRows} dòng vào orders_address`
        );

        const [nullDistrictWardOrders] = await connection.query(
          `
          SELECT id_order, address, source
          FROM orders_address
          WHERE district IS NULL AND ward IS NULL AND address IS NOT NULL
          `
        );
        console.log(
          `[standardizeAddresses] Tìm thấy ${nullDistrictWardOrders.length} bản ghi orders_address có district và ward null`
        );

        const transportPromises = nullDistrictWardOrders.map((order) =>
          limit(async () => {
            const { id_order, address } = order;
            console.log(
              `[standardizeAddresses] Tìm nhà xe cho id_order ${id_order}: ${address}`
            );
            const transportResult = await findTransportCompany(
              address,
              orderDetailMap.get(id_order)?.date_delivery,
              orderDetailMap.get(id_order)?.travel_time || 15,
              id_order,
              "",
              ""
            );
            if (transportResult.DcGiaohang) {
              console.log(
                `[standardizeAddresses] Nhà xe tìm thấy cho id_order ${id_order}: ${JSON.stringify(
                  transportResult
                )}`
              );
              return {
                MaPX: id_order,
                DcGiaohang: transportResult.DcGiaohang,
                District: transportResult.District,
                Ward: transportResult.Ward,
                Source: "TransportDB",
                isEmpty: false,
                distance: null,
                travel_time: null,
                priority: noteInfo.priority || 0,
                deliveryDate: noteInfo.deliveryDate || "",
                cargoType: noteInfo.cargoType || "",
              };
            }
            console.log(
              `[standardizeAddresses] Không tìm thấy nhà xe cho id_order ${id_order}`
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
            order.distance,
            order.travel_time,
          ]);
          const [transportInsertResult] = await connection.query(
            `INSERT INTO orders_address (id_order, address, district, ward, source, distance, travel_time)
             VALUES ?
             ON DUPLICATE KEY UPDATE
               address = IF(VALUES(address) != '', VALUES(address), address),
               district = IF(VALUES(district) IS NOT NULL, VALUES(district), district),
               ward = IF(VALUES(ward) IS NOT NULL, VALUES(ward), ward),
               source = IF(VALUES(source) IS NOT NULL, VALUES(source), source),
               distance = VALUES(distance),
               travel_time = VALUES(travel_time)`,
            [transportValues]
          );
          console.log(
            `[standardizeAddresses] Lưu ${transportInsertResult.affectedRows} dòng từ nhà xe vào orders_address`
          );

          validResults.push(...transportResults);
        }
      }
      await connection.end();
    } else {
      console.log("[standardizeAddresses] Không có kết quả hợp lệ để lưu");
    }

    console.log(
      `[standardizeAddresses] Hoàn tất, thời gian thực thi: ${
        Date.now() - startTime
      }ms, OpenAI calls: ${openAICalls}`
    );
    return validResults;
  } catch (error) {
    console.error(
      `[standardizeAddresses] Lỗi tổng quát: ${error.message}, stack: ${error.stack}`
    );
    throw error;
  }
}

// TÍNH THỜI GIAN VẬN CHUYỂN THEO KHUNG GIỜ
function getTravelTimeByTimeFrame(SOKM, dateDelivery) {
  let time = dateDelivery
    ? moment(dateDelivery, "DD/MM/YYYY HH:mm:ss").tz("Asia/Ho_Chi_Minh")
    : moment().tz("Asia/Ho_Chi_Minh");

  if (!time.isValid()) {
    console.warn(
      `[getTravelTimeByTimeFrame] date_delivery không hợp lệ: ${dateDelivery}, sử dụng thời gian hiện tại`
    );
    time = moment().tz("Asia/Ho_Chi_Minh");
  }

  const workHours = {
    weekdays: [
      { start: 8.0, end: 12.0 },
      { start: 13.5, end: 17.75 },
    ],
    saturday: [
      { start: 8.0, end: 12.0 },
      { start: 13.5, end: 16.5 },
    ],
  };

  const adjustToWorkingHours = (inputTime) => {
    let adjustedTime = inputTime.clone();
    const dayOfWeek = adjustedTime.day();

    if (dayOfWeek === 0) {
      adjustedTime.add(1, "day").startOf("day").add(8, "hours");
      console.log(
        `[getTravelTimeByTimeFrame] Thời gian rơi vào Chủ Nhật, chuyển sang 8h sáng thứ Hai: ${adjustedTime.format(
          "DD/MM/YYYY HH:mm:ss"
        )}`
      );
      return adjustedTime;
    }

    const isSaturday = dayOfWeek === 6;
    const schedule = isSaturday ? workHours.saturday : workHours.weekdays;

    const hour = adjustedTime.hour();
    const minute = adjustedTime.minute();
    const currentTime = hour + minute / 60;

    const isWorkingHour = schedule.some(
      (slot) => currentTime >= slot.start && currentTime < slot.end
    );

    if (!isWorkingHour) {
      if (currentTime < schedule[0].start) {
        adjustedTime.startOf("day").add(8, "hours");
      } else if (
        currentTime >= schedule[0].end &&
        currentTime < schedule[1].start
      ) {
        adjustedTime.startOf("day").add(13.5, "hours");
      } else {
        adjustedTime.add(1, "day").startOf("day").add(8, "hours");
        if (adjustedTime.day() === 0) {
          adjustedTime.add(1, "day").startOf("day").add(8, "hours");
        }
      }
      console.log(
        `[getTravelTimeByTimeFrame] Thời gian ngoài giờ làm việc, điều chỉnh sang: ${adjustedTime.format(
          "DD/MM/YYYY HH:mm:ss"
        )}`
      );
    }

    return adjustedTime;
  };

  time = adjustToWorkingHours(time);

  const hour = time.hour();
  const minute = time.minute();
  const currentTime = hour + minute / 60;

  const timeFrames = [
    {
      start: 6.5,
      end: 9.0,
      maxDistance: 10,
      travelTimeRange: [25, 35],
      name: "Sáng",
    },
    {
      start: 11.0,
      end: 13.0,
      maxDistance: 12,
      travelTimeRange: [15, 20],
      name: "Trưa",
    },
    {
      start: 13.5,
      end: 17.75,
      maxDistance: 10,
      travelTimeRange: [30, 40],
      name: "Chiều",
    },
  ];

  if (time.day() === 6) {
    timeFrames[2].end = 16.5;
  }

  const frame = timeFrames.find(
    (f) => currentTime >= f.start && currentTime < f.end
  );

  if (!frame) {
    console.warn(
      `[getTravelTimeByTimeFrame] Không tìm thấy khung giờ phù hợp cho thời điểm ${time.format(
        "HH:mm"
      )}, sử dụng mặc định`
    );
    return 20;
  }

  if (SOKM > frame.maxDistance) {
    console.warn(
      `[getTravelTimeByTimeFrame] SOKM (${SOKM} km) vượt quá khoảng cách hợp lý (${frame.maxDistance} km) cho khung giờ ${frame.name}`
    );
    return frame.travelTimeRange[1];
  }

  const [minTime, maxTime] = frame.travelTimeRange;
  const ratio = SOKM / frame.maxDistance;
  const travelTime = Math.round(minTime + (maxTime - minTime) * ratio);

  console.log(
    `[getTravelTimeByTimeFrame] Khung giờ: ${
      frame.name
    }, SOKM: ${SOKM}, travel_time: ${travelTime} phút, dựa trên thời gian: ${time.format(
      "DD/MM/YYYY HH:mm:ss"
    )}`
  );
  return travelTime;
}

// ========================================================== UPDATE ORDER FUNCTIONS ==========================================================
// CẬP NHẬT TRẠNG THÁI ĐƠN HÀNG
async function updatePriorityStatus(io) {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);

    const [orders] = await connection.query(
      `
      SELECT oa.id_order, o.date_delivery
      FROM orders_address oa
      JOIN orders o ON oa.id_order = o.id_order
      WHERE oa.status = 0
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
        AND o.date_delivery IS NOT NULL
      `
    );
    console.log(`Số lượng đơn hàng cần cập nhật: ${orders.length}`);

    const validOrders = orders.filter((order) => {
      const deliveryMoment = moment(
        order.date_delivery,
        "DD/MM/YYYY HH:mm:ss",
        true
      );
      if (!deliveryMoment.isValid()) {
        console.warn(
          `[updatePriorityStatus] date_delivery không hợp lệ cho ${order.id_order}: ${order.date_delivery}`
        );
        return false;
      }
      return deliveryMoment.isBefore(moment().subtract(15, "minutes"));
    });

    console.log(`Số lượng đơn hàng hợp lệ: ${validOrders.length}`);

    if (validOrders.length > 0) {
      const [result] = await connection.query(
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
      console.log(
        "[updatePriorityStatus] Số dòng ảnh hưởng:",
        result.affectedRows
      );

      if (result.affectedRows > 0) {
        const [updatedOrders] = await connection.query(
          `
          SELECT oa.id_order, oa.address, oa.status
          FROM orders_address oa
          WHERE oa.status = 1
          `
        );
        console.log(
          `Số lượng đơn hàng đã cập nhật trạng thái: ${updatedOrders.length}`
        );
      }
    }

    await connection.end();
    console.log(
      `[updatePriorityStatus] Thực thi trong ${Date.now() - startTime}ms`
    );

    if (validOrders.length > 0 && io) {
      io.emit("statusUpdated", {
        message: "Đã cập nhật trạng thái đơn hàng",
        updatedCount: validOrders.length,
      });
    }
  } catch (error) {
    console.error("[updatePriorityStatus] Lỗi:", error.message, error.stack);
  }
}

// CẬP NHẬT ĐỊA CHỈ CHUẨN HÓA
async function updateStandardizedAddresses(data) {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);

    const validOrderIds = await getValidOrderIds();
    const validOrders = data.filter((order) => validOrderIds.has(order.MaPX));

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

      const values = validOrders
        .filter((order) => order.DcGiaohang !== undefined)
        .map((order) => {
          const current = addressMap.get(order.MaPX) || {
            distance: null,
            travel_time: null,
          };
          return [
            order.MaPX,
            order.DcGiaohang || "",
            order.District,
            order.Ward,
            order.Source,
            order.distance || 0,
            order.travel_time || 0,
            order.addressChanged ? current.distance : null,
            order.addressChanged ? current.travel_time : null,
          ];
        });

      console.log(
        "[updateStandardizedAddresses] Dữ liệu cập nhật:",
        validOrders.map((order) => ({
          MaPX: order.MaPX,
          DcGiaohang: order.DcGiaohang,
          District: order.District,
          Ward: order.Ward,
          Source: order.Source,
          distance: order.distance,
          travel_time: order.travel_time,
          addressChanged: order.addressChanged,
        }))
      );

      if (values.length > 0) {
        const [result] = await connection.query(
          `
          INSERT INTO orders_address (
            id_order, address, district, ward, source, 
            distance, travel_time, old_distance, old_travel_time
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            address = IF(VALUES(address) != '', VALUES(address), address),
            district = IF(VALUES(district) IS NOT NULL, VALUES(district), district),
            ward = IF(VALUES(ward) IS NOT NULL, VALUES(ward), ward),
            source = IF(VALUES(source) IS NOT NULL, VALUES(source), source),
            distance = VALUES(distance),
            travel_time = VALUES(travel_time),
            old_distance = IF(VALUES(old_distance) IS NOT NULL, VALUES(old_distance), old_distance),
            old_travel_time = IF(VALUES(old_travel_time) IS NOT NULL, VALUES(old_travel_time), old_travel_time)
          `,
          values.flat()
        );
        console.log(
          "[updateStandardizedAddresses] Số dòng ảnh hưởng khi lưu vào cơ sở dữ liệu (orders_address):",
          result.affectedRows
        );
      }

      const invalidOrders = data.filter(
        (order) => !validOrderIds.has(order.MaPX)
      );
      if (invalidOrders.length > 0) {
        console.warn(
          "[updateStandardizedAddresses] Các MaPX không tồn tại trong bảng orders:",
          invalidOrders.map((order) => order.MaPX)
        );
      }
    } else {
      console.warn(
        "[updateStandardizedAddresses] Không có đơn hàng hợp lệ để lưu vào orders_address"
      );
    }

    await connection.end();
    console.log(
      `[updateStandardizedAddresses] Thực thi trong ${Date.now() - startTime}ms`
    );
  } catch (error) {
    console.error("[updateStandardizedAddresses] Lỗi:", error.message);
    throw error;
  }
}

// ĐỒNG BỘ TRẠNG THÁI ĐƠN HÀNG
async function syncOrderStatus() {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);

    const [orders] = await connection.query(
      `
      SELECT id_order
      FROM orders
      WHERE status IN ('Chờ xác nhận giao/lấy hàng', 'Đang giao/lấy hàng')
        AND date_delivery IS NOT NULL
      `
    );
    console.log(`Số lượng đơn hàng cần đồng bộ trạng thái: ${orders.length}`);

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

// CẬP NHẬT TRẠNG THÁI ĐƠN HÀNG
async function updateOrderStatusToCompleted() {
  const startTime = Date.now();
  try {
    const connection = await mysql.createConnection(dbConfig);

    const [orders] = await connection.query(
      `
      SELECT id_order, status
      FROM orders
      WHERE status IN ('Chờ xác nhận giao/lấy hàng', 'Đang giao/lấy hàng')
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
            currentStatus: order.status,
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

    const updates = [];
    for (const order of results) {
      const { MaPX, Tinhtranggiao, currentStatus } = order;

      if (
        currentStatus === "Chờ xác nhận giao/lấy hàng" &&
        Tinhtranggiao === "Đang giao/lấy hàng"
      ) {
        updates.push(["Đang giao/lấy hàng", MaPX]);
      } else if (
        currentStatus === "Đang giao/lấy hàng" &&
        Tinhtranggiao === "Hoàn thành"
      ) {
        updates.push(["Hoàn thành", MaPX]);
      }
    }

    if (updates.length > 0) {
      const [updateResult] = await connection.query(
        `
        UPDATE orders
        SET status = ?
        WHERE id_order = ?
        `,
        updates.flat()
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

// ========================================================== SELECT ORDER FUNCTIONS ==========================================================
// SẮP XẾP ĐƠN HÀNG
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

    const sortedResults = parsedResults.sort((a, b) => {
      let priorityA, priorityB;

      // Tiêu chí 1: Kiểm tra lỗi dữ liệu (thiếu district, ward, distance, hoặc travel_time)
      if (
        !a.district ||
        !a.ward ||
        a.distance === null ||
        a.travel_time === null
      ) {
        priorityA = 100;
      }
      // Tiêu chí 2: Kiểm tra distance > 100 km
      else if (a.distance > 100) {
        priorityA = 99; // Xếp trước các đơn lỗi nhưng sau các đơn bình thường
      }
      // Các tiêu chí hiện tại
      else if (a.priority === 2) {
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
      } else if (b.distance > 100) {
        priorityB = 99;
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

      // Nếu cả hai đều có distance > 100 km, áp dụng các tiêu chí phụ và thêm date_delivery
      if (priorityA === 99 && priorityB === 99) {
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

        const timeToDeadlineA = a.delivery_deadline
          ? moment(a.delivery_deadline).diff(moment(), "minutes")
          : 999999;
        const timeToDeadlineB = b.delivery_deadline
          ? moment(b.delivery_deadline).diff(moment(), "minutes")
          : 999999;
        if (timeToDeadlineA !== timeToDeadlineB) {
          return timeToDeadlineA - timeToDeadlineB;
        }

        const distanceA = a.distance !== null ? a.distance : 999999;
        const distanceB = b.distance !== null ? b.distance : 999999;
        if (distanceA !== distanceB) {
          return distanceA - distanceB;
        }

        const travelTimeA = a.travel_time !== null ? a.travel_time : 999999;
        const travelTimeB = b.travel_time !== null ? b.travel_time : 999999;
        if (travelTimeA !== travelTimeB) {
          return travelTimeA - travelTimeB;
        }

        // Tiêu chí phụ: Sắp xếp theo date_delivery tăng dần
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

        return a.id_order.localeCompare(b.id_order);
      }

      // Các tiêu chí phụ cho các đơn hàng khác
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

      const timeToDeadlineA = a.delivery_deadline
        ? moment(a.delivery_deadline).diff(moment(), "minutes")
        : 999999;
      const timeToDeadlineB = b.delivery_deadline
        ? moment(b.delivery_deadline).diff(moment(), "minutes")
        : 999999;
      if (timeToDeadlineA !== timeToDeadlineB) {
        return timeToDeadlineA - timeToDeadlineB;
      }

      const distanceA = a.distance !== null ? a.distance : 999999;
      const distanceB = b.distance !== null ? b.distance : 999999;
      if (distanceA !== distanceB) {
        return distanceA - distanceB;
      }

      const travelTimeA = a.travel_time !== null ? a.travel_time : 999999;
      const travelTimeB = b.travel_time !== null ? b.travel_time : 999999;
      if (travelTimeA !== travelTimeB) {
        return travelTimeA - travelTimeB;
      }

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

      return a.id_order.localeCompare(b.id_order);
    });

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

// SẮP XẾP ĐƠN HÀNG (PHIÊN BẢN 2)
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

    const sortedResults = parsedResults.sort((a, b) => {
      let priorityA, priorityB;

      // Tiêu chí 1: Kiểm tra lỗi dữ liệu (thiếu district, ward, distance, hoặc travel_time)
      if (
        !a.district ||
        !a.ward ||
        a.distance === null ||
        a.travel_time === null
      ) {
        priorityA = 100;
      }
      // Tiêu chí 2: Kiểm tra distance > 100 km
      else if (a.distance > 100) {
        priorityA = 99; // Xếp trước các đơn lỗi nhưng sau các đơn bình thường
      }
      // Các tiêu chí hiện tại
      else if (a.priority === 2) {
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
      } else if (b.distance > 100) {
        priorityB = 99;
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

      // Nếu cả hai đều có distance > 100 km, áp dụng các tiêu chí phụ và thêm date_delivery
      if (priorityA === 99 && priorityB === 99) {
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

        const timeToDeadlineA = a.delivery_deadline
          ? moment(a.delivery_deadline).diff(moment(), "minutes")
          : 999999;
        const timeToDeadlineB = b.delivery_deadline
          ? moment(b.delivery_deadline).diff(moment(), "minutes")
          : 999999;
        if (timeToDeadlineA !== timeToDeadlineB) {
          return timeToDeadlineA - timeToDeadlineB;
        }

        const distanceA = a.distance !== null ? a.distance : 999999;
        const distanceB = b.distance !== null ? b.distance : 999999;
        if (distanceA !== distanceB) {
          return distanceA - distanceB;
        }

        const travelTimeA = a.travel_time !== null ? a.travel_time : 999999;
        const travelTimeB = b.travel_time !== null ? b.travel_time : 999999;
        if (travelTimeA !== travelTimeB) {
          return travelTimeA - travelTimeB;
        }

        // Tiêu chí phụ: Sắp xếp theo date_delivery tăng dần
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

        return a.id_order.localeCompare(b.id_order);
      }

      // Các tiêu chí phụ cho các đơn hàng khác
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

      const timeToDeadlineA = a.delivery_deadline
        ? moment(a.delivery_deadline).diff(moment(), "minutes")
        : 999999;
      const timeToDeadlineB = b.delivery_deadline
        ? moment(b.delivery_deadline).diff(moment(), "minutes")
        : 999999;
      if (timeToDeadlineA !== timeToDeadlineB) {
        return timeToDeadlineA - timeToDeadlineB;
      }

      const distanceA = a.distance !== null ? a.distance : 999999;
      const distanceB = b.distance !== null ? b.distance : 999999;
      if (distanceA !== distanceB) {
        return distanceA - distanceB;
      }

      const travelTimeA = a.travel_time !== null ? a.travel_time : 999999;
      const travelTimeB = b.travel_time !== null ? b.travel_time : 999999;
      if (travelTimeA !== travelTimeB) {
        return travelTimeA - travelTimeB;
      }

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

      return a.id_order.localeCompare(b.id_order);
    });

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

// =========================================================== PHÂN TÍCH GHI CHÚ GIAO HÀNG ===========================================================
// PHÂN TÍCH GHI CHÚ GIAO HÀNG
async function analyzeDeliveryNote() {
    const startTime = Date.now();
    try {
        // Kết nối cơ sở dữ liệu
        const connection = await createConnectionWithRetry();

        // Truy vấn các đơn hàng chưa được phân tích
        const [orders] = await connection.query(
            `
            SELECT o.id_order, o.delivery_note, o.date_delivery, oa.travel_time
            FROM orders o
            LEFT JOIN orders_address oa ON o.id_order = oa.id_order
            WHERE o.status = 'Chờ xác nhận giao/lấy hàng'
                AND o.priority = 0
                AND o.delivery_deadline IS NULL
                AND o.analyzed = 0
                AND o.delivery_note IS NOT NULL
                AND o.delivery_note != ''
                AND o.date_delivery IS NOT NULL
            `
        );

        console.log(`Số lượng đơn hàng cần phân tích ghi chú: ${orders.length}`);

        if (orders.length === 0) {
            console.log("[analyzeDeliveryNote] Không có đơn hàng có ghi chú cần phân tích");
            await connection.end();
            console.log(`[analyzeDeliveryNote] Thực thi trong ${Date.now() - startTime}ms`);
            return;
        }

        // Khởi tạo danh sách để lưu các đơn hàng đã phân tích và cần cập nhật
        const analyzedOrders = [];
        const priorityUpdates = [];
        const limit = pLimit(50);

        // Hàm phân tích ghi chú giao hàng
        const parseDeliveryNote = (note, travelTime, order) => {
            try {
                // Luôn thêm đơn hàng vào analyzedOrders
                analyzedOrders.push([order.id_order]);

                // Kiểm tra tính hợp lệ của date_delivery
                const deliveryTime = moment(order.date_delivery, "DD/MM/YYYY HH:mm:ss").tz("Asia/Ho_Chi_Minh");
                if (!deliveryTime.isValid()) {
                    console.warn(`Đơn ${order.id_order}: date_delivery không hợp lệ: ${order.date_delivery}`);
                    return { id_order: order.id_order, priority: 0, delivery_deadline: null };
                }

                // Khởi tạo biến
                let deliveryDeadline = null;
                let priority = 0;
                let deliveryDateMoment = deliveryTime.clone();
                let hasKeyword = false; // Theo dõi xem có từ khóa thời gian không

                // Danh sách ngày lễ Việt Nam (năm 2025/2026)
                const holidays = [
                    { name: "giỗ tổ hùng vương", date: moment("29/03/2025", "DD/MM/YYYY").tz("Asia/Ho_Chi_Minh") },
                    { name: "ngày giải phóng", date: moment("30/04/2025", "DD/MM/YYYY").tz("Asia/Ho_Chi_Minh") },
                    { name: "quốc tế lao động", date: moment("01/05/2025", "DD/MM/YYYY").tz("Asia/Ho_Chi_Minh") },
                    { name: "quốc khánh", date: moment("02/09/2025", "DD/MM/YYYY").tz("Asia/Ho_Chi_Minh") },
                    { name: "tết nguyên đán", date: moment("30/01/2026", "DD/MM/YYYY").tz("Asia/Ho_Chi_Minh") },
                    { name: "trăng rằm trung thu", date: moment("12/09/2025", "DD/MM/YYYY").tz("Asia/Ho_Chi_Minh") },
                    { name: "noel", date: moment("25/12/2025", "DD/MM/YYYY").tz("Asia/Ho_Chi_Minh") },
                ];

                // Chuẩn hóa ghi chú
                const normalizedNote = note
                    .toLowerCase()
                    .replace(/trc|truoc|trước khi/g, "trước")
                    .replace(/gap|gấp|giaogap|giao gấp|nhanh nhất|lien|liền/g, "gấp")
                    .replace(/sn|sớm nhất|sớm nhé|som nhat|som nhe|sớm nha/g, "sớm")
                    .replace(/nhah|nhan|nhanh len|nhanh nha|nhanh nhất|nhanh nhat/g, "nhanh")
                    .replace(/sang|sáng|sang mai|sáng mai|sang hom nay|sáng hôm nay/g, "sáng")
                    .replace(/chiu|chiu nay|chiều|chiều hnay|chiều hôm nay|chieu hom nay|chieu nay/g, "chiều")
                    .replace(/toi|toi nay|tối nay|tối|tối hnay|tối hôm nay|toi hom nay/g, "tối")
                    .replace(/hom nay|hnay|trong ngày|ngay hom nay|ngày hôm nay/g, "hôm nay")
                    .replace(/tuan nay|trong tuan|trong tuần|tuần này|tuan ni/g, "tuần này")
                    .replace(/tuan sau|trong tuan sau|trong tuần sau|tuần sau|qua tuần|tuan toi|tuần tới/g, "tuần sau")
                    .replace(/dau tuan sau|đầu tuần sau|dau tuan toi|đầu tuần tới/g, "đầu tuần sau")
                    .replace(/cuoi tuan nay|cuối tuần này|cuoi tuan ni|cuối tuần ni/g, "cuối tuần này")
                    .replace(/cuoi tuan sau|cuối tuần sau|cuoi tuan toi|cuối tuần tới/g, "cuối tuần sau")
                    .replace(/thang sau|tháng sau|trong thang sau|trong tháng sau|thang toi|tháng tới/g, "tháng sau")
                    .replace(/dau thang sau|đầu tháng sau|dau thang toi|đầu tháng tới/g, "đầu tháng sau")
                    .replace(/giua thang sau|giữa tháng sau|giua thang toi|giữa tháng tới/g, "giữa tháng sau")
                    .replace(/cuoi thang sau|cuối tháng sau|cuoi thang toi|cuối tháng tới/g, "cuối tháng sau")
                    .replace(/mai|ngay mai|bữa sau|bua sau|hom sau|hôm sau|ngày mai/g, "ngày mai")
                    .replace(/mot|ngay mot|mốt|2 ngày nữa|hai ngay nua|2 bữa nữa|hai bua nua|2 hôm nữa|hai hôm nữa/g, "ngày mốt")
                    .replace(/ngay kia|ngày kia|3 ngay nua|3 hôm nữa|ba ngay nua|ba hom nua/g, "ngày kia")
                    .replace(/(\d+)\s*ngay nua|\d+\s*hom nua|\d+\s*buoi nua/g, (match, days) => `${days} ngày nữa`)
                    .replace(/giao nhanh trong ngay|giao nhanh hom nay/g, "gấp hôm nay")
                    .replace(/giao truoc tet|giao trước tết|truoc tet|trước tết|tết nguyên đán|tết âm lịch/g, "trước tết")
                    .replace(/giao noel|giao vào noel|vao noel|vào noel/g, "noel")
                    .replace(/giao truoc trung thu|giao trước trung thu|truoc trung thu|trước trung thu|trăng rằm/g, "trước trung thu")
                    .replace(/giao truoc gio to|giao trước giỗ tổ|truoc gio to|trước giỗ tổ|giỗ tổ hùng vương/g, "trước giỗ tổ")
                    .replace(/giao ngay 30\/4|giao vào 30\/4|ngày 30\/4|30 thang 4|ngày giải phóng/g, "ngày giải phóng")
                    .replace(/giao ngay 1\/5|giao vào 1\/5|ngày 1\/5|1 thang 5|quốc tế lao động/g, "quốc tế lao động")
                    .replace(/giao ngay 2\/9|giao vào 2\/9|ngày 2\/9|2 thang 9|quốc khánh/g, "quốc khánh")
                    .replace(/giao khi khach o nha|giao khi khách ở nhà|khi khach o nha|khách ở nhà/g, "khi khách ở nhà")
                    .replace(/giao sau khi lien he|giao sau khi liên hệ|sau khi lien he|sau khi liên hệ/g, "sau khi liên hệ")
                    .replace(/thu hai tuần này|thứ hai tuần này|thu 2 tuần này|thứ 2 tuần này|thu hai tuan ni|thứ hai tuan ni/g, "thứ hai tuần này")
                    .replace(/thu ba tuần này|thứ ba tuần này|thu 3 tuần này|thứ 3 tuần này|thu ba tuan ni|thứ ba tuan ni/g, "thứ ba tuần này")
                    .replace(/thu tu tuần này|thứ tư tuần này|thu 4 tuần này|thứ 4 tuần này|thu tu tuan ni|thứ tư tuan ni/g, "thứ tư tuần này")
                    .replace(/thu nam tuần này|thứ năm tuần này|thu 5 tuần này|thứ 5 tuần này|thu nam tuan ni|thứ năm tuan ni/g, "thứ năm tuần này")
                    .replace(/thu sau tuần này|thứ sáu tuần này|thu 6 tuần này|thứ 6 tuần này|thu sau tuan ni|thứ sáu tuan ni/g, "thứ sáu tuần này")
                    .replace(/thu bay tuần này|thứ bảy tuần này|thu 7 tuần này|thứ 7 tuần này|thu bay tuan ni|thứ bảy tuan ni/g, "thứ bảy tuần này")
                    .replace(/chu nhat tuần này|chủ nhật tuần này|cn tuần này|chu nhat tuan ni|chủ nhật tuan ni/g, "chủ nhật tuần này")
                    .replace(/thu hai tuần sau|thứ hai tuần sau|thu 2 tuần sau|thứ 2 tuần sau|thu hai tuan toi|thứ hai tuan toi/g, "thứ hai tuần sau")
                    .replace(/thu ba tuần sau|thứ ba tuần sau|thu 3 tuần sau|thứ 3 tuần sau|thu ba tuan toi|thứ ba tuan toi/g, "thứ ba tuần sau")
                    .replace(/thu tu tuần sau|thứ tư tuần sau|thu 4 tuần sau|thứ 4 tuần sau|thu tu tuan toi|thứ tư tuan toi/g, "thứ tư tuần sau")
                    .replace(/thu nam tuần sau|thứ năm tuần sau|thu 5 tuần sau|thứ 5 tuần sau|thu nam tuan toi|thứ năm tuan toi/g, "thứ năm tuần sau")
                    .replace(/thu sau tuần sau|thứ sáu tuần sau|thu 6 tuần sau|thứ 6 tuần sau|thu sau tuan toi|thứ sáu tuan toi/g, "thứ sáu tuần sau")
                    .replace(/thu bay tuần sau|thứ bảy tuần sau|thu 7 tuần sau|thứ 7 tuần sau|thu bay tuan toi|thứ bảy tuan toi/g, "thứ bảy tuần sau")
                    .replace(/chu nhat tuần sau|chủ nhật tuần sau|cn tuần sau|chu nhat tuan toi|chủ nhật tuan toi/g, "chủ nhật tuần sau")
                    .replace(/khoang|khoảng|tu|tu\s*den|từ\s*đến/g, "đến")
                    .replace(/\s+/g, " ")
                    .trim();

                // Phân tích ghi chú
                const noteInfo = parseDeliveryNoteForAddress(normalizedNote);
                const { timeHint, priority: notePriority, deliveryDate } = noteInfo;
                priority = notePriority;

                // Log để kiểm tra đầu ra của parseDeliveryNoteForAddress
                console.log(`Đơn ${order.id_order}: normalizedNote="${normalizedNote}", timeHint="${timeHint}", deliveryDate="${deliveryDate}", notePriority=${notePriority}`);

                // Xác định ngày giao hàng từ ghi chú
                const specificDateMatch = normalizedNote.match(/(?:giao ngày|giao vào ngày|giao lúc|giao)\s*(\d{2}[./]\d{2}(?:[./]\d{4})?)/i);
                if (specificDateMatch) {
                    let dateStr = specificDateMatch[1];
                    dateStr = dateStr.replace(/\./g, "/");
                    if (!dateStr.includes("/202")) {
                        dateStr += `/2025`;
                    }
                    deliveryDateMoment = moment(dateStr, "DD/MM/YYYY").tz("Asia/Ho_Chi_Minh");
                    hasKeyword = true;
                } else if (deliveryDate) {
                    hasKeyword = true;
                    const now = moment().tz("Asia/Ho_Chi_Minh");
                    switch (deliveryDate.toLowerCase()) {
                        case "hôm nay":
                            deliveryDateMoment = now.clone();
                            break;
                        case "ngày mai":
                            deliveryDateMoment = now.clone().add(1, "day");
                            break;
                        case "ngày mốt":
                            deliveryDateMoment = now.clone().add(2, "days");
                            break;
                        case "ngày kia":
                            deliveryDateMoment = now.clone().add(3, "days");
                            break;
                        case "tuần này":
                            deliveryDateMoment = now.clone().endOf("week").subtract(1, "day");
                            break;
                        case "cuối tuần này":
                            deliveryDateMoment = now.clone().endOf("week").subtract(1, "day");
                            break;
                        case "tuần sau":
                            deliveryDateMoment = now.clone().add(1, "week").endOf("week").subtract(1, "day");
                            break;
                        case "đầu tuần sau":
                            deliveryDateMoment = now.clone().add(1, "week").startOf("week").add(1, "day");
                            break;
                        case "cuối tuần sau":
                            deliveryDateMoment = now.clone().add(1, "week").endOf("week").subtract(1, "day");
                            break;
                        case "tháng sau":
                            deliveryDateMoment = now.clone().add(1, "month").endOf("month");
                            break;
                        case "đầu tháng sau":
                            deliveryDateMoment = now.clone().add(1, "month").startOf("month");
                            break;
                        case "giữa tháng sau":
                            deliveryDateMoment = now.clone().add(1, "month").startOf("month").add(15, "days");
                            break;
                        case "cuối tháng sau":
                            deliveryDateMoment = now.clone().add(1, "month").endOf("month");
                            break;
                        case "trước tết":
                            deliveryDateMoment = holidays.find(h => h.name === "tết nguyên đán").date.clone().subtract(1, "day");
                            priority = 2;
                            break;
                        case "noel":
                            deliveryDateMoment = holidays.find(h => h.name === "noel").date.clone();
                            break;
                        case "trước trung thu":
                            deliveryDateMoment = holidays.find(h => h.name === "trăng rằm trung thu").date.clone().subtract(1, "day");
                            priority = 2;
                            break;
                        case "trước giỗ tổ":
                            deliveryDateMoment = holidays.find(h => h.name === "giỗ tổ hùng vương").date.clone().subtract(1, "day");
                            priority = 2;
                            break;
                        case "ngày giải phóng":
                            deliveryDateMoment = holidays.find(h => h.name === "ngày giải phóng").date.clone();
                            break;
                        case "quốc tế lao động":
                            deliveryDateMoment = holidays.find(h => h.name === "quốc tế lao động").date.clone();
                            break;
                        case "quốc khánh":
                            deliveryDateMoment = holidays.find(h => h.name === "quốc khánh").date.clone();
                            break;
                        case "thứ hai":
                        case "thứ hai tuần này":
                            deliveryDateMoment = now.clone();
                            while (deliveryDateMoment.day() !== 1) deliveryDateMoment.add(1, "day");
                            if (deliveryDateMoment.isBefore(now)) deliveryDateMoment.add(7, "days");
                            break;
                        case "thứ ba":
                        case "thứ ba tuần này":
                            deliveryDateMoment = now.clone();
                            while (deliveryDateMoment.day() !== 2) deliveryDateMoment.add(1, "day");
                            if (deliveryDateMoment.isBefore(now)) deliveryDateMoment.add(7, "days");
                            break;
                        case "thứ tư":
                        case "thứ tư tuần này":
                            deliveryDateMoment = now.clone();
                            while (deliveryDateMoment.day() !== 3) deliveryDateMoment.add(1, "day");
                            if (deliveryDateMoment.isBefore(now)) deliveryDateMoment.add(7, "days");
                            break;
                        case "thứ năm":
                        case "thứ năm tuần này":
                            deliveryDateMoment = now.clone();
                            while (deliveryDateMoment.day() !== 4) deliveryDateMoment.add(1, "day");
                            if (deliveryDateMoment.isBefore(now)) deliveryDateMoment.add(7, "days");
                            break;
                        case "thứ sáu":
                        case "thứ sáu tuần này":
                            deliveryDateMoment = now.clone();
                            while (deliveryDateMoment.day() !== 5) deliveryDateMoment.add(1, "day");
                            if (deliveryDateMoment.isBefore(now)) deliveryDateMoment.add(7, "days");
                            break;
                        case "thứ bảy":
                        case "thứ bảy tuần này":
                            deliveryDateMoment = now.clone();
                            while (deliveryDateMoment.day() !== 6) deliveryDateMoment.add(1, "day");
                            if (deliveryDateMoment.isBefore(now)) deliveryDateMoment.add(7, "days");
                            break;
                        case "chủ nhật":
                        case "chủ nhật tuần này":
                            deliveryDateMoment = now.clone();
                            while (deliveryDateMoment.day() !== 0) deliveryDateMoment.add(1, "day");
                            if (deliveryDateMoment.isBefore(now)) deliveryDateMoment.add(7, "days");
                            break;
                        case "thứ hai tuần sau":
                            deliveryDateMoment = now.clone().add(1, "week");
                            while (deliveryDateMoment.day() !== 1) deliveryDateMoment.add(1, "day");
                            break;
                        case "thứ ba tuần sau":
                            deliveryDateMoment = now.clone().add(1, "week");
                            while (deliveryDateMoment.day() !== 2) deliveryDateMoment.add(1, "day");
                            break;
                        case "thứ tư tuần sau":
                            deliveryDateMoment = now.clone().add(1, "week");
                            while (deliveryDateMoment.day() !== 3) deliveryDateMoment.add(1, "day");
                            break;
                        case "thứ năm tuần sau":
                            deliveryDateMoment = now.clone().add(1, "week");
                            while (deliveryDateMoment.day() !== 4) deliveryDateMoment.add(1, "day");
                            break;
                        case "thứ sáu tuần sau":
                            deliveryDateMoment = now.clone().add(1, "week");
                            while (deliveryDateMoment.day() !== 5) deliveryDateMoment.add(1, "day");
                            break;
                        case "thứ bảy tuần sau":
                            deliveryDateMoment = now.clone().add(1, "week");
                            while (deliveryDateMoment.day() !== 6) deliveryDateMoment.add(1, "day");
                            break;
                        case "chủ nhật tuần sau":
                            deliveryDateMoment = now.clone().add(1, "week");
                            while (deliveryDateMoment.day() !== 0) deliveryDateMoment.add(1, "day");
                            break;
                        default:
                            const daysMatch = deliveryDate.match(/(\d+)\s*ngày nữa/);
                            if (daysMatch) {
                                const days = parseInt(daysMatch[1], 10);
                                deliveryDateMoment = now.clone().add(days, "days");
                            } else {
                                hasKeyword = false; // Nếu deliveryDate không khớp, đặt lại hasKeyword
                            }
                            break;
                    }
                }

                // Kiểm tra ngày lễ và Chủ nhật
                const isHoliday = holidays.some(h => deliveryDateMoment.isSame(h.date, "day"));
                const isSunday = deliveryDateMoment.day() === 0;
                if (hasKeyword && (isHoliday || isSunday)) {
                    do {
                        deliveryDateMoment.add(1, "day");
                    } while (deliveryDateMoment.day() === 0 || holidays.some(h => deliveryDateMoment.isSame(h.date, "day")));
                }

                // Xử lý các trường hợp đặc biệt
                if (normalizedNote.includes("khi khách ở nhà") || normalizedNote.includes("sau khi liên hệ")) {
                    hasKeyword = true;
                    priority = 1;
                    deliveryDeadline = null;
                } else if (normalizedNote.includes("gấp") || normalizedNote.includes("gấp hôm nay")) {
                    hasKeyword = true;
                    priority = 2;
                    deliveryDeadline = deliveryTime.clone().add(travelTime + 15, "minutes");
                } else if (normalizedNote.includes("sớm")) {
                    hasKeyword = true;
                    priority = 1;
                    const now = moment().tz("Asia/Ho_Chi_Minh");
                    deliveryDateMoment = now.clone();
                    deliveryDeadline = now.hour() < 14
                        ? now.clone().add(3, "hours")
                        : deliveryDateMoment.clone().startOf("day").add(17, "hours").add(45, "minutes");
                }

                // Xử lý thời gian giao hàng từ ghi chú (nếu chưa có deadline)
                if (hasKeyword && timeHint && !deliveryDeadline) {
                    const timeRangeMatch = normalizedNote.match(/(\d{1,2})\s*đến\s*(\d{1,2})\s*h/i);
                    if (timeRangeMatch) {
                        let startHour = parseInt(timeRangeMatch[1], 10);
                        let endHour = parseInt(timeRangeMatch[2], 10);
                        const minute = 0;
                        if (normalizedNote.includes("sáng") && endHour < 12) {
                            // Giữ nguyên
                        } else if (normalizedNote.includes("chiều") && startHour < 12) {
                            startHour += 12;
                            endHour += 12;
                        } else if (normalizedNote.includes("tối") && startHour < 12) {
                            startHour += 12;
                            endHour += 12;
                        }
                        deliveryDeadline = deliveryDateMoment
                            .clone()
                            .startOf("day")
                            .add(endHour, "hours")
                            .add(minute, "minutes");
                    } else {
                        switch (timeHint.toLowerCase()) {
                            case "sáng":
                                deliveryDeadline = deliveryDateMoment.clone().startOf("day").add(10, "hours");
                                break;
                            case "chiều":
                                deliveryDeadline = deliveryDateMoment.clone().startOf("day").add(15, "hours");
                                break;
                            case "tối":
                                deliveryDeadline = deliveryDateMoment.clone().startOf("day").add(17, "hours").add(45, "minutes");
                                break;
                            default:
                                const timeMatch = timeHint.match(/(\d{1,2})(?::(\d{2}))?(h|am|pm)?/i);
                                if (timeMatch) {
                                    let hour = parseInt(timeMatch[1], 10);
                                    const minute = timeMatch[2] ? parseInt(timeMatch[2], 10) : 0;
                                    const period = timeMatch[3] ? timeMatch[3].toLowerCase() : "h";
                                    if (period === "pm" && hour < 12) hour += 12;
                                    else if (period === "am" && hour === 12) hour = 0;
                                    else if (normalizedNote.includes("tối") && hour < 12) hour += 12;
                                    else if (normalizedNote.includes("chiều") && hour < 12) hour += 12;
                                    deliveryDeadline = deliveryDateMoment
                                        .clone()
                                        .startOf("day")
                                        .add(hour, "hours")
                                        .add(minute, "minutes");
                                }
                                break;
                        }
                    }
                }

                // Điều chỉnh delivery_deadline theo thời gian làm việc
                if (hasKeyword && deliveryDeadline) {
                    const isSaturday = deliveryDeadline.day() === 6;
                    const startOfDay = deliveryDeadline.clone().startOf("day");
                    const workStart = startOfDay.clone().add(8, "hours");
                    const workEnd = isSaturday
                        ? startOfDay.clone().add(16, "hours").add(30, "minutes")
                        : startOfDay.clone().add(17, "hours").add(45, "minutes");
                    const lunchStart = startOfDay.clone().add(12, "hours");
                    const lunchEnd = startOfDay.clone().add(13, "hours").add(30, "minutes");

                    const isHoliday = holidays.some(h => deliveryDeadline.isSame(h.date, "day"));
                    const isSunday = deliveryDeadline.day() === 0;
                    if (isHoliday || isSunday) {
                        do {
                            deliveryDeadline.add(1, "day");
                            deliveryDeadline = deliveryDeadline.clone().startOf("day").add(8, "hours");
                        } while (deliveryDeadline.day() === 0 || holidays.some(h => deliveryDeadline.isSame(h.date, "day")));
                        deliveryDateMoment = deliveryDeadline.clone().startOf("day");
                    }

                    if (deliveryDeadline.isSameOrAfter(lunchStart) && deliveryDeadline.isBefore(lunchEnd)) {
                        deliveryDeadline = lunchEnd.clone();
                    }

                    if (deliveryDeadline.isBefore(workStart)) {
                        deliveryDeadline = workStart.clone();
                    } else if (deliveryDeadline.isAfter(workEnd)) {
                        do {
                            deliveryDeadline.add(1, "day");
                            deliveryDeadline = deliveryDeadline.clone().startOf("day").add(8, "hours");
                        } while (deliveryDeadline.day() === 0 || holidays.some(h => deliveryDeadline.isSame(h.date, "day")));
                        deliveryDateMoment = deliveryDeadline.clone().startOf("day");
                    }

                    if (deliveryDeadline.isBefore(deliveryTime)) {
                        deliveryDeadline = deliveryTime.clone().add(travelTime + 15, "minutes");
                    }

                    const timeToDeadline = deliveryDeadline.diff(moment(), "minutes");
                    if (timeToDeadline < 0) {
                        priority = 2;
                    } else if (timeToDeadline <= 60) {
                        priority = 2;
                    } else if (timeToDeadline <= 90) {
                        priority = 1;
                    } else if (priority === 0) {
                        priority = 1;
                    }
                }

                // Nếu không có từ khóa, trả về null và priority = 0
                if (!hasKeyword) {
                    console.log(`Đơn ${order.id_order}: Không tìm thấy từ khóa thời gian, gán delivery_deadline=null, priority=0`);
                    return { id_order: order.id_order, priority: 0, delivery_deadline: null };
                }

                // Đảm bảo delivery_deadline là null hoặc chuỗi DATETIME hợp lệ
                if (deliveryDeadline && !moment(deliveryDeadline, "YYYY-MM-DD HH:mm:ss", true).isValid()) {
                    console.warn(`Đơn ${order.id_order}: delivery_deadline không hợp lệ (${deliveryDeadline}), gán null`);
                    deliveryDeadline = null;
                }

                const result = {
                    id_order: order.id_order,
                    priority,
                    delivery_deadline: deliveryDeadline ? deliveryDeadline.format("YYYY-MM-DD HH:mm:ss") : null,
                };

                // Log kết quả cuối cùng của parseDeliveryNote
                console.log(`Đơn ${order.id_order}: Kết quả parseDeliveryNote: priority=${result.priority}, delivery_deadline=${result.delivery_deadline}`);

                return result;
            } catch (error) {
                console.error(`Lỗi phân tích đơn ${order.id_order}: ${error.message}`);
                return { id_order: order.id_order, priority: 0, delivery_deadline: null };
            }
        };

        // Phân tích ghi chú theo batch
        const batchSize = 50;
        for (let i = 0; i < orders.length; i += batchSize) {
            const batch = orders.slice(i, i + batchSize);
            const batchPromises = batch.map((order, index) =>
                limit(async () => {
                    console.log(`Xử lý đơn ${order.id_order} (hàng ${i + index + 1}): delivery_note="${order.delivery_note}", date_delivery="${order.date_delivery}", travel_time=${order.travel_time}`);
                    const result = parseDeliveryNote(order.delivery_note, order.travel_time || 15, order);
                    if (!result) {
                        console.warn(`Đơn ${order.id_order}: Không trả về kết quả hợp lệ`);
                        return;
                    }
                    console.log(
                        `[analyzeDeliveryNote] Đã phân tích đơn ${order.id_order}: delivery_note="${order.delivery_note}", priority=${result.priority}, delivery_deadline=${result.delivery_deadline}`
                    );
                    if (result.priority > 0 || result.delivery_deadline) {
                        // Kiểm tra giá trị delivery_deadline trước khi đẩy vào priorityUpdates
                        if (result.delivery_deadline === '0' || (result.delivery_deadline && !moment(result.delivery_deadline, "YYYY-MM-DD HH:mm:ss", true).isValid())) {
                            console.warn(`Đơn ${order.id_order}: delivery_deadline không hợp lệ (${result.delivery_deadline}), gán null`);
                            result.delivery_deadline = null;
                        }
                        priorityUpdates.push([
                            result.priority,
                            result.delivery_deadline,
                            result.id_order,
                        ]);
                    }
                })
            );
            await Promise.all(batchPromises);
        }

        // Cập nhật analyzed, priority, và delivery_deadline trong một truy vấn
        if (analyzedOrders.length > 0) {
            console.log(`[analyzeDeliveryNote] Số đơn hàng đã phân tích: ${analyzedOrders.length}`);
            
            // Tạo truy vấn UPDATE với CASE
            const idOrders = analyzedOrders.map(([id_order]) => id_order);
            let updateQuery = `
                UPDATE orders
                SET 
                    analyzed = 1,
                    priority = CASE id_order
            `;
            let deliveryDeadlineCase = `
                    delivery_deadline = CASE id_order
            `;
            const queryParams = [];

            analyzedOrders.forEach(([id_order]) => {
                const update = priorityUpdates.find(([_, __, id]) => id === id_order) || [0, null, id_order];
                updateQuery += ` WHEN ? THEN ?`;
                deliveryDeadlineCase += ` WHEN ? THEN ?`;
                queryParams.push(id_order, update[0], id_order, update[1]);
            });

            updateQuery += `
                    ELSE priority END,
            `;
            deliveryDeadlineCase += `
                    ELSE delivery_deadline END
            `;

            updateQuery += deliveryDeadlineCase + `
                WHERE id_order IN (${idOrders.map(() => '?').join(',')})
            `;
            queryParams.push(...idOrders);

            const [updateResult] = await connection.query(updateQuery, queryParams);
            console.log(`[analyzeDeliveryNote] Số dòng cập nhật: ${updateResult.affectedRows}`);

            // Thống kê số đơn không có từ khóa
            const noKeywordCount = analyzedOrders.length - priorityUpdates.length;
            console.log(`[analyzeDeliveryNote] Số đơn không có từ khóa: ${noKeywordCount}`);
        } else {
            console.log("[analyzeDeliveryNote] Không có đơn hàng nào được phân tích");
        }

        await connection.end();
        console.log(`[analyzeDeliveryNote] Thực thi trong ${Date.now() - startTime}ms`);
    } catch (error) {
        console.error("[analyzeDeliveryNote] Lỗi:", error.message, error.stack);
        throw error;
    }
}

// ================================================================== CHƯƠNG TRÌNH CHÍNH ==================================================
// CHƯƠNG TRÌNH CHÍNH
async function main(page = 1, io) {
  const startTime = Date.now();
  let api2Calls = 0,
    openAICalls = 0,
    tomtomCalls = 0;

  try {
    console.log(
      "🚀 Khởi động công cụ giao hàng lúc:",
      moment().tz("Asia/Ho_Chi_Minh").format()
    );
    console.log(
      "================================================================="
    );

    console.log("📦 Bước 1: Lấy và lưu đơn hàng...");
    const orders = await fetchAndSaveOrders();
    api2Calls += orders.length;
    console.log(`✅ Đã lưu ${orders.length} đơn hàng vào orders`);
    console.log(
      "================================================================="
    );

    console.log("🔄 Bước 2: Đồng bộ trạng thái đơn hàng...");
    await syncOrderStatus();
    api2Calls += orders.length;
    console.log("✅ Đã đồng bộ trạng thái đơn hàng");
    console.log(
      "================================================================="
    );

    console.log("📋 Bước 3: Cập nhật trạng thái đơn hàng hoàn thành...");
    await updateOrderStatusToCompleted();
    api2Calls += orders.length;
    console.log("✅ Đã cập nhật trạng thái các đơn hàng hoàn thành");
    console.log(
      "================================================================="
    );

    console.log("🗺️ Bước 4: Chuẩn hóa và ánh xạ địa chỉ...");
    const connection = await createConnectionWithRetry();
    const [unstandardizedOrders] = await connection.query(
      `
      SELECT o.id_order AS MaPX, o.address AS DcGiaohang, 
             o.old_address, o.DiachiTruSo
      FROM orders o
      LEFT JOIN orders_address oa ON o.id_order = oa.id_order
      WHERE o.status = 'Chờ xác nhận giao/lấy hàng'
        AND oa.id_order IS NULL
      `,
      []
    );
    await connection.end();

    console.log(
      "[main] Dữ liệu đơn hàng cần chuẩn hóa:",
      unstandardizedOrders.map((o) => ({
        MaPX: o.MaPX,
        DcGiaohang: o.DcGiaohang,
        DiachiTruSo: o.DiachiTruSo,
      }))
    );
    const ordersToStandardize = unstandardizedOrders.map((order) => ({
      MaPX: order.MaPX,
      DcGiaohang: order.DcGiaohang,
      DiachiTruSo: order.DiachiTruSo, // Thêm DiachiTruSo
      isEmpty: !order.DcGiaohang,
      addressChanged: order.DcGiaohang !== order.old_address,
    }));
    console.log(
      "[main] Số đơn hàng cần chuẩn hóa:",
      ordersToStandardize.length
    );

    let standardizedOrders = [];
    if (ordersToStandardize.length > 0) {
      standardizedOrders = await standardizeAddresses(ordersToStandardize);
      openAICalls += standardizedOrders.length;
      console.log(`[main] Đã chuẩn hóa ${standardizedOrders.length} đơn hàng`);
    } else {
      console.log("[main] Không có đơn hàng nào cần chuẩn hóa");
    }
    console.log(
      "================================================================="
    );

    console.log("💾 Bước 5: Cập nhật địa chỉ chuẩn hóa...");
    if (standardizedOrders.length > 0) {
      await updateStandardizedAddresses(standardizedOrders);
      console.log("✅ Đã cập nhật địa chỉ chuẩn hóa");
    } else {
      console.log("[main] Không có địa chỉ chuẩn hóa để cập nhật");
    }
    console.log(
      "================================================================="
    );

    console.log("📏 Bước 6: Tính toán khoảng cách và thời gian...");
    await calculateDistances();
    tomtomCalls += ordersToStandardize.length;
    console.log("✅ Đã tính toán khoảng cách và thời gian");
    console.log(
      "================================================================="
    );

    console.log("📝 Bước 7: Phân tích ghi chú đơn hàng...");
    await analyzeDeliveryNote();
    console.log("✅ Đã phân tích ghi chú và cập nhật ưu tiên");
    console.log(
      "================================================================="
    );

    console.log("⏫ Bước 8: Cập nhật trạng thái ưu tiên đơn hàng...");
    await updatePriorityStatus(io);
    console.log("✅ Đã cập nhật trạng thái ưu tiên");
    console.log(
      "================================================================="
    );

    console.log(`🔍 Bước 9: Lấy đơn hàng gần nhất (trang ${page})...`);
    const groupedOrders = await groupOrders(page);
    console.log(
      "================================================================="
    );

    console.log("📊 Thống kê API calls:");
    console.log(`- API_2 calls: ${api2Calls}`);
    console.log(`- OpenAI calls: ${openAICalls}`);
    console.log(`- TomTom calls: ${tomtomCalls}`);

    if (io) {
      io.emit("ordersUpdated", {
        message: "Danh sách đơn hàng đã được cập nhật",
        data: groupedOrders,
        nextRunTime: getNextCronRunTime(),
      });
      console.log(`[main] Đã gửi danh sách đơn hàng qua Socket.io`);
    }

    console.log("🏁 Công cụ giao hàng hoàn tất.");
    console.log(`[main] Thực thi trong ${Date.now() - startTime}ms`);

    lastRunTime = moment().tz("Asia/Ho_Chi_Minh").format();

    return groupedOrders;
  } catch (error) {
    console.error("[main] Lỗi:", error.message, error.stack);
    throw error;
  }
}

// CHẠY CHƯƠNG TRÌNH LẦN ĐẦU
main(1, io).catch((error) =>
  console.error("Lỗi khi chạy main lần đầu:", error.message)
);

// CẬP NHẬT ĐƠN HÀNG MỚI MỖI 5 PHÚT
cron.schedule("*/5 * * * *", () => {
  console.log(
    "Chạy quy trình giao hàng lúc:",
    moment().tz("Asia/Ho_Chi_Minh").format()
  );
  main(1, io).catch((error) =>
    console.error("Lỗi khi chạy main:", error.message)
  );
});

// CẬP NHẬT TRẠNG THÁI ĐƠN HÀNG MỖI 15 PHÚT
cron.schedule("*/15 * * * *", () => {
  console.log(
    "Chạy quy trình đồng bộ trạng thái lúc:",
    moment().tz("Asia/Ho_Chi_Minh").format()
  );
  syncOrderStatus().catch((error) =>
    console.error("Lỗi khi chạy syncOrderStatus:", error.message)
  );
});

// ================================================================== ROUTER ==================================================
// SẮP XẾP ĐƠN HÀNG
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

// SẮP XẾP ĐƠN HÀNG 2
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

// XỬ LÝ ĐƠN HÀNG
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

// LẤY DANH SÁCH QUẬN VÀ PHƯỜNG
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

// TÌM KIẾM ĐƠN HÀNG
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
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
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

// LỌC ĐƠN HÀNG
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
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
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

// LỌC ĐƠN HÀNG NÂNG CAO
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

    const filters = ["o.status = 'Chờ xác nhận giao/lấy hàng'"];
    if (dateCondition) filters.push(dateCondition);
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

// LỌC ĐƠN HÀNG THEO NGÀY
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

// TÌM KIẾM ĐƠN HÀNG THEO ID
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
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
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

// LẤY ĐƠN HÀNG QUÁ HẠN
app.get("/orders/overdue", async (req, res) => {
  try {
    const connection = await mysql.createConnection(dbConfig);

    const [rows] = await connection.query(
      `
      SELECT o.*, a.address, a.district, a.ward, a.distance, a.travel_time, a.status AS address_status
      FROM orders o
      LEFT JOIN orders_address a ON o.id_order = a.id_order
      WHERE a.status = 1
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
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

// TÌM KIẾM ĐƠN HÀNG THEO ID
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
        AND o.status = 'Chờ xác nhận giao/lấy hàng'
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
// KHỞI TẠO SERVER
server.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
