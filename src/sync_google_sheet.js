require('dotenv').config();
console.log('OPENAI_API_KEY (sync_google_sheet.js):', process.env.OPENAI_API_KEY);

const { google } = require('googleapis');
const { standardizeAddresses, updateStandardizedAddresses } = require('./delivery-tool');

console.log('GOOGLE_CREDENTIALS_PATH (sync_google_sheet.js):', process.env.GOOGLE_CREDENTIALS_PATH);

// Cấu hình Google Sheets API
const auth = new google.auth.GoogleAuth({
  keyFile: process.env.GOOGLE_CREDENTIALS_PATH,
  scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
});

const sheets = google.sheets({ version: 'v4', auth });

// ID của Google Sheet
const SPREADSHEET_ID = '12KF-YT8YBKr6fk3_DNj10pay1BfVZML7nvmr0CkEUM4';
const SHEET_NAME = 'ds nha xe'; // Tên sheet đã sửa đúng
const RANGE = 'A5:G'; // Range không bao gồm tên sheet, sẽ được thêm sau

async function getSheetIdByName(spreadsheetId, sheetName) {
  try {
    const response = await sheets.spreadsheets.get({
      spreadsheetId: spreadsheetId,
    });

    const sheetsList = response.data.sheets;
    const sheet = sheetsList.find(s => s.properties.title === sheetName);

    if (!sheet) {
      throw new Error(`Không tìm thấy sheet có tên: ${sheetName}`);
    }

    return sheet.properties.sheetId;
  } catch (error) {
    console.error('Lỗi khi lấy sheet ID:', error.message);
    throw error;
  }
}

async function fetchGoogleSheetData() {
  try {
    // Lấy sheet ID từ tên sheet
    const sheetId = await getSheetIdByName(SPREADSHEET_ID, SHEET_NAME);
    console.log(`Sheet ID của "${SHEET_NAME}": ${sheetId}`);

    // Tạo range với sheet ID
    const rangeWithSheetId = `${sheetId}!${RANGE}`;
    console.log(`Range được sử dụng: ${rangeWithSheetId}`);

    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: rangeWithSheetId,
    });

    const rows = response.data.values;
    if (!rows || rows.length === 0) {
      console.log('Không tìm thấy dữ liệu trong Google Sheet.');
      return [];
    }

    // Chuyển đổi dữ liệu từ Google Sheet thành định dạng tương thích
    const transportCompanies = rows
      .filter(row => row[1] && row[2]) // Lọc bỏ các dòng thiếu tên nhà xe hoặc địa chỉ
      .map((row, index) => ({
        tempId: `TEMP_${index + 1}`, // Tạo tempId tạm thời để liên kết dữ liệu
        name: row[1] || '', // Cột B: Tên nhà xe
        address: row[2] || '', // Cột C: Địa chỉ
        phone: row[3] || '', // Cột D: Số điện thoại
        departureTime: row[4] || '', // Cột E: Giờ đi
        status: row[5] || '', // Cột F: Tình trạng
        note: row[6] || '', // Cột G: Ghi chú
        isEmpty: !row[2], // Đánh dấu địa chỉ rỗng
      }));

    console.log(`Đã lấy ${transportCompanies.length} nhà xe từ Google Sheet.`);
    return transportCompanies;
  } catch (error) {
    console.error('Lỗi khi lấy dữ liệu từ Google Sheet:', error.message);
    throw error;
  }
}

async function syncGoogleSheetToDatabase() {
  try {
    console.log('Bắt đầu đồng bộ dữ liệu từ Google Sheet (Danh sách nhà xe)...');

    // Bước 1: Lấy dữ liệu từ Google Sheet
    const transportCompanies = await fetchGoogleSheetData();
    if (transportCompanies.length === 0) {
      console.log('Không có dữ liệu để đồng bộ.');
      return;
    }

    // Bước 2: Chuẩn hóa địa chỉ bằng OpenAI
    console.log('Chuẩn hóa địa chỉ...');
    const ordersToStandardize = transportCompanies.map(company => ({
      MaPX: company.tempId,
      DcGiaohang: company.address,
      isEmpty: company.isEmpty,
    }));
    const standardizedOrders = await standardizeAddresses(ordersToStandardize);
    console.log(`Đã chuẩn hóa ${standardizedOrders.length} địa chỉ.`);

    // Bước 3: Kết hợp dữ liệu chuẩn hóa với thông tin nhà xe
    const mergedData = standardizedOrders.map(order => {
      const company = transportCompanies.find(c => c.tempId === order.MaPX);
      return {
        name: company.name,
        address: company.address,
        standardizedAddress: order.DcGiaohang,
        district: order.District,
        ward: order.Ward,
        phone: company.phone,
        departureTime: company.departureTime,
        status: company.status,
        note: company.note,
        source: order.Source,
      };
    });

    // Bước 4: Lưu dữ liệu vào bảng transport_companies
    console.log('Lưu dữ liệu vào database...');
    await updateStandardizedAddresses(mergedData, true);
    console.log('Đã lưu dữ liệu vào database.');

    console.log('Đồng bộ dữ liệu từ Google Sheet hoàn tất.');
  } catch (error) {
    console.error('Lỗi trong quá trình đồng bộ:', error.message);
    throw error;
  }
}

// Chạy script
syncGoogleSheetToDatabase().catch((err) => {
  console.error('Lỗi:', err);
  process.exit(1);
});