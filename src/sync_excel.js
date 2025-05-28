import "dotenv/config";
import XLSX from "xlsx";
import {
  standardizeAddresses,
  updateStandardizedAddresses,
} from "./delivery-tool.js";

const EXCEL_FILE_PATH = "ds_nha_xe.xls";
const SHEET_NAME = "VŨNG TÀU";

async function fetchExcelData() {
  try {
    const workbook = XLSX.readFile(EXCEL_FILE_PATH);
    const worksheet = workbook.Sheets[SHEET_NAME];
    if (!worksheet) {
      throw new Error(`Không tìm thấy sheet có tên: ${SHEET_NAME}`);
    }

    const rows = XLSX.utils.sheet_to_json(worksheet, { header: 1, range: 4 });

    if (!rows || rows.length === 0) {
      console.log("Không tìm thấy dữ liệu trong file Excel.");
      return [];
    }

    const transportCompanies = rows
      .filter((row) => row[1] && row[2])
      .map((row, index) => ({
        tempId: `TEMP_${index + 1}`,
        name: (row[1] || "").toUpperCase(),
        address: (row[2] || "").toLowerCase(),
        phone: row[3] || "",
        departureTime: row[4] || "",
        status: row[5] || "",
        note: row[6] || "",
        isEmpty: !row[2],
      }));

    console.log(`Đã lấy ${transportCompanies.length} nhà xe từ file Excel.`);
    return transportCompanies;
  } catch (error) {
    console.error("Lỗi khi lấy dữ liệu từ file Excel:", error.message);
    throw error;
  }
}

async function syncExcelToDatabase() {
  try {
    console.log("Bắt đầu đồng bộ dữ liệu từ file Excel (Danh sách nhà xe)...");

    const transportCompanies = await fetchExcelData();
    if (transportCompanies.length === 0) {
      console.log("Không có dữ liệu để đồng bộ.");
      return;
    }

    console.log("Chuẩn hóa địa chỉ...");
    const ordersToStandardize = transportCompanies.map((company) => ({
      MaPX: company.tempId,
      DcGiaohang: company.address,
      isEmpty: company.isEmpty,
    }));
    const standardizedOrders = await standardizeAddresses(ordersToStandardize);
    console.log(`Đã chuẩn hóa ${standardizedOrders.length} địa chỉ.`);

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

    console.log("Lưu dữ liệu vào database...");
    await updateStandardizedAddresses(mergedData, true);
    console.log("Đã lưu dữ liệu vào database.");

    console.log("Đồng bộ dữ liệu từ file Excel hoàn tất.");
  } catch (error) {
    console.error("Lỗi trong quá trình đồng bộ:", error.message);
    throw error;
  }
}

syncExcelToDatabase().catch((err) => {
  console.error("Lỗi:", err);
  process.exit(1);
});
