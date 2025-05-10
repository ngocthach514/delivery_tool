require('dotenv').config();

const { google } = require('googleapis');
const { standardizeAddresses, updateStandardizedAddresses } = require('./delivery-tool');

const auth = new google.auth.GoogleAuth({
  keyFile: process.env.GOOGLE_CREDENTIALS_PATH,
  scopes: ['https://www.googleapis.com/auth/spreadsheets'],
});

const sheets = google.sheets({ version: 'v4', auth });

const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const RANGE = process.env.RANGE;

async function getAllSheetNames(spreadsheetId) {
  try {
    const response = await sheets.spreadsheets.get({
      spreadsheetId: spreadsheetId,
      fields: 'sheets.properties',
    });

    const sheetNames = response.data.sheets.map(s => s.properties.title);
    console.log('Danh sách sheets:', sheetNames);
    return sheetNames;
  } catch (error) {
    console.error('Lỗi khi lấy danh sách sheets:', error.message);
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
      .filter(row => row[1] && row[2])
      .map((row, index) => ({
        tempId: `TEMP_${sheetName}_${index + 1}`, // Thêm sheetName vào tempId để tránh trùng lặp
        name: (row[1] || '').toUpperCase(), // Tên nhà xe in hoa
        address: (row[2] || '').toLowerCase(), // Địa chỉ chữ thường
        phone: (row[3] || '').substring(0, 10), // Cắt ngắn số điện thoại để tránh lỗi "Data too long"
        departureTime: row[4] || '',
        status: row[5] || '',
        note: row[6] || '',
        isEmpty: !row[2],
      }));

    console.log(`Đã lấy ${transportCompanies.length} nhà xe từ sheet ${sheetName}.`);
    return transportCompanies;
  } catch (error) {
    console.error(`Lỗi khi lấy dữ liệu từ sheet ${sheetName}:`, error.message);
    throw error;
  }
}

async function syncGoogleSheetToDatabase() {
  try {
    console.log('Bắt đầu đồng bộ dữ liệu từ Google Sheet (Danh sách nhà xe)...');

    // Lấy danh sách tất cả sheets
    const sheetNames = await getAllSheetNames(SPREADSHEET_ID);
    if (!sheetNames || sheetNames.length === 0) {
      console.log('Không tìm thấy sheet nào trong Google Sheet.');
      return;
    }

    // Xử lý và lưu dữ liệu từng sheet riêng lẻ
    for (const sheetName of sheetNames) {
      console.log(`Xử lý sheet: ${sheetName}...`);

      // Lấy dữ liệu từ sheet hiện tại
      const transportCompanies = await fetchGoogleSheetData(SPREADSHEET_ID, sheetName);
      if (transportCompanies.length === 0) {
        console.log(`Không có dữ liệu để đồng bộ từ sheet ${sheetName}.`);
        continue; // Bỏ qua sheet này và chuyển sang sheet tiếp theo
      }

      // Chuẩn hóa địa chỉ
      console.log(`Chuẩn hóa địa chỉ từ sheet ${sheetName}...`);
      const ordersToStandardize = transportCompanies.map(company => ({
        MaPX: company.tempId,
        DcGiaohang: company.address,
        isEmpty: company.isEmpty,
      }));
      const standardizedOrders = await standardizeAddresses(ordersToStandardize);
      console.log(`Đã chuẩn hóa ${standardizedOrders.length} địa chỉ từ sheet ${sheetName}.`);

      // Chuẩn bị dữ liệu để lưu
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
          note: order.Source,
        };
      });

      // Lưu dữ liệu từ sheet hiện tại vào database
      console.log(`Lưu dữ liệu từ sheet ${sheetName} vào database...`);
      await updateStandardizedAddresses(mergedData, true);
      console.log(`Đã lưu dữ liệu từ sheet ${sheetName} vào database.`);
    }

    console.log('Đồng bộ dữ liệu từ Google Sheet hoàn tất.');
  } catch (error) {
    console.error('Lỗi trong quá trình đồng bộ:', error.message);
    throw error;
  }
}

syncGoogleSheetToDatabase().catch((err) => {
  console.error('Lỗi:', err);
  process.exit(1);
});