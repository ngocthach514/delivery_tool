import "dotenv/config";
import { google } from "googleapis";
import {
  standardizeAddresses,
  updateStandardizedAddresses,
} from "./delivery-tool.js";

const auth = new google.auth.GoogleAuth({
  keyFile: process.env.GOOGLE_CREDENTIALS_PATH,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const sheets = google.sheets({ version: "v4", auth });

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const RANGE = process.env.RANGE;

async function getAllSheetNames(spreadsheetId) {
  try {
    const response = await sheets.spreadsheets.get({
      spreadsheetId: spreadsheetId,
      fields: "sheets.properties",
    });

    const sheetNames = response.data.sheets.map((s) => s.properties.title);
    console.log("Danh sách sheets:", sheetNames);
    return sheetNames;
  } catch (error) {
    console.error("Lỗi khi lấy danh sách sheets:", error.message);
    throw error;
  }
}

async function fetchGoogleSheetData(spreadsheetId, sheetName) {
  try {
    const rangeWithSheetName = `${sheetName}!${RANGE}`;
    console.log(`Range được sử dụng: ${rangeWithSheetName}`);

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: spreadsheetId,
      range: rangeWithSheetName,
    });

    const rows = response.data.values;
    console.log(`Dữ liệu thô từ sheet ${sheetName}:`, rows);
    if (!rows || rows.length === 0) {
      console.log(`Không tìm thấy dữ liệu trong sheet ${sheetName}.`);
      return [];
    }

    const transportCompanies = rows
      .filter((row) => row[1] && row[2])
      .map((row, index) => ({
        tempId: `TEMP_${sheetName}_${index + 1}`,
        name: (row[1] || "").toUpperCase(),
        address: (row[2] || "").toLowerCase(),
        phone: (row[3] || "").substring(0, 10),
        departureTime: row[4] || "",
        status: row[5] || "",
        note: row[6] || "",
        isEmpty: !row[2],
      }));

    console.log(
      `Đã lấy ${transportCompanies.length} nhà xe từ sheet ${sheetName}.`
    );
    return transportCompanies;
  } catch (error) {
    console.error(`Lỗi khi lấy dữ liệu từ sheet ${sheetName}:`, error.message);
    throw error;
  }
}

async function syncGoogleSheetToDatabase() {
  try {
    console.log(
      "Bắt đầu đồng bộ dữ liệu từ Google Sheet (Danh sách nhà xe)..."
    );

    const sheetNames = await getAllSheetNames(SPREADSHEET_ID);
    if (!sheetNames || sheetNames.length === 0) {
      console.log("Không tìm thấy sheet nào trong Google Sheet.");
      return;
    }

    for (const sheetName of sheetNames) {
      console.log(`Xử lý sheet: ${sheetName}...`);

      const transportCompanies = await fetchGoogleSheetData(
        SPREADSHEET_ID,
        sheetName
      );
      if (transportCompanies.length === 0) {
        console.log(`Không có dữ liệu để đồng bộ từ sheet ${sheetName}.`);
        continue;
      }

      console.log(`Chuẩn hóa địa chỉ từ sheet ${sheetName}...`);
      const ordersToStandardize = transportCompanies.map((company) => ({
        MaPX: company.tempId,
        DcGiaohang: company.address,
        isEmpty: company.isEmpty,
      }));
      const standardizedOrders = await standardizeAddresses(
        ordersToStandardize
      );
      console.log(
        `Đã chuẩn hóa ${standardizedOrders.length} địa chỉ từ sheet ${sheetName}.`
      );

      const mergedData = standardizedOrders.map((order) => {
        const company = transportCompanies.find((c) => c.tempId === order.MaPX);
        return {
          name: company.name,
          address: company.address,
          standardizedAddress: order.DcGiaohang,
          district: order.District,
          ward: order.Ward,
          phone: company.phone,
          departureTime: company.departureTime,
          status: company.status,
          note: order.Source,
        };
      });

      console.log(`Lưu dữ liệu từ sheet ${sheetName} vào database...`);
      await updateStandardizedAddresses(mergedData, true);
      console.log(`Đã lưu dữ liệu từ sheet ${sheetName} vào database.`);
    }

    console.log("Đồng bộ dữ liệu từ Google Sheet hoàn tất.");
  } catch (error) {
    console.error("Lỗi trong quá trình đồng bộ:", error.message);
    throw error;
  }
}

syncGoogleSheetToDatabase().catch((err) => {
  console.error("Lỗi:", err);
  process.exit(1);
});
