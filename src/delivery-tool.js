const axios = require('axios');
const mysql = require('mysql2/promise');
const { OpenAI } = require('openai');
const pLimitModule = require('p-limit');

// Ensure pLimit is a function
const pLimit = typeof pLimitModule === 'function' ? pLimitModule : pLimitModule.default;

// Database connection configuration
const dbConfig = {
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: 'delivery_data'
};

// OpenAI configuration
const openai = new OpenAI({
    apiKey: process.env.OPENAI_API_KEY
});

// API endpoints
const API_1 = 'http://192.168.117.222:8096/NKC/Delivery/GetVoucher?type=chogiao';
const API_2 = 'http://192.168.117.222:8096/NKC/Web/SearchVoucher?qc=X241113010-N&pc=';

// Step 1: Fetch orders and save to database
async function fetchAndSaveOrders() {
    try {
        // Fetch API 1
        const response1 = await axios.get(API_1);
        const orders = response1.data; // Assuming array of orders with MaPX
        console.log('API 1 orders:', orders.length);
        console.log('API 1 full response:', JSON.stringify(orders, null, 2));

        // Fetch API 2 in parallel with limit
        const limit = pLimit(10); // Limit to 10 concurrent requests
        const api2Promises = orders.map(order => 
            limit(() => axios.get(`${API_2}${order.MaPX}`)
                .then(res => ({
                    MaPX: order.MaPX,
                    DcGiaohang: res.data.DcGiaohang // Assuming DcGiaohang field
                }))
                .catch(err => {
                    console.error(`Error fetching API 2 for MaPX ${order.MaPX}:`, err.message);
                    return null; // Return null for failed requests
                }))
        );

        // Use Promise.allSettled to handle all promises, even if some fail
        const settledResults = await Promise.allSettled(api2Promises);
        const results = settledResults
            .filter(result => result.status === 'fulfilled' && result.value !== null)
            .map(result => result.value);
        console.log('API 2 successful results:', results.length);
        console.log('API 2 results:', JSON.stringify(results, null, 2));

        // Save to database
        const connection = await mysql.createConnection(dbConfig);
        if (results.length > 0) {
            const values = results.map(order => [order.MaPX, order.DcGiaohang]);
            const [insertResult] = await connection.query(
                'INSERT INTO orders (id_order, address) VALUES ? ON DUPLICATE KEY UPDATE address = VALUES(address)',
                [values]
            );
            console.log('Database insert affected rows (orders):', insertResult.affectedRows);
        }
        
        await connection.end();
        return results;
    } catch (error) {
        console.error('Error in fetchAndSaveOrders:', error.message);
        throw error;
    }
}

// Step 2: Standardize addresses using OpenAI
async function standardizeAddresses(orders) {
    try {
        const prompt = `
Bạn là một AI chuyên phân tích địa chỉ ở Việt Nam
Hiện tại tôi đang có một list các địa chỉ cần phải chuẩn hóa cùng một format

Dưới đây là ví dụ:
Input: là một mảng các địa chỉ được viết lộn xộn, có cả những thông tin dư thừa
[
{"MaPX" : "X241019081-N", "DcGiaohang" : "559A Đường 3/2 P8 Q10 ( Mr Hậu Kho 0906.272.346)"},
{"MaPX" : "X241019082-N", "DcGiaohang" : "Số 621 Phạm Văn Chí, Phường 7, Quận 6, TP. HCM"},
{"MaPX" : "X241019083-N", "DcGiaohang" : "191 BÙI THỊ XUÂN, PHƯỜNG 6, QUẬN TÂN BÌNH"},
{"MaPX" : "X241019084-N", "DcGiaohang" : "XE ANH KHOA 1390 Võ Văn Kiệt (Góc Chu Văn An) 0936845050 ( A Duy )/ 0909769393 ( A Chiến) Chiều :Giao trước 18g30"},
{"MaPX" : "X241019085-N", "DcGiaohang" : "1107 Tự Lập, Phường 4, Quận Tân Bình"}
]

Output: Chuẩn một định dạng để tôi có thể phân chia quận huyện 1 cách dễ dàng
[
{"MaPX" : "X241019081-N", "DcGiaohang" : "559A Đ. 3 Tháng 2, Phường 8, Quận 10, Hồ Chí Minh, Việt Nam", "District" : "Quận 10", "Ward" : "Phường 8"},
{"MaPX" : "X241019082-N", "DcGiaohang" : "621 Đ. Phạm Văn Chí, Phường 7, Quận 6, Hồ Chí Minh, Việt Nam", "District" : "Quận 6", "Ward" : "Phường 7"},
{"MaPX" : "X241019083-N", "DcGiaohang" : "191 Bùi Thị Xuân, Phường 1, Tân Bình, Hồ Chí Minh, Việt Nam", "District" : "Quận Tân Bình", "Ward" : "Phường 1"},
{"MaPX" : "X241019084-N", "DcGiaohang" : "Chành xe Bảo Tín - Phan Thiết, 1390 Đ. Võ Văn Kiệt, Phường 1, Quận 6, Hồ Chí Minh, Việt Nam", "District" : "Quận 6", "Ward" : "Phường 1"},
{"MaPX" : "X241019085-N", "DcGiaohang" : "1107 Tự Lập, Phường 4, Tân Bình, Hồ Chí Minh, Việt Nam", "District" : "Quận Tân Bình", "Ward" : "Phường 4"}
]

ĐÂY Là List
${JSON.stringify(orders)}

Trả Về Cho tôi 1 chuỗi JSON thôi, nhanh chóng không cần dài dòng phân tích
`;

        const completion = await openai.chat.completions.create({
            model: 'gpt-4o',
            messages: [{ role: 'system', content: prompt }]
        });

        let content = completion.choices[0].message.content;
        content = content.replace(/```json\n?|\n?```/g, '').trim();
        console.log('OpenAI response:', content);
        return JSON.parse(content);
    } catch (error) {
        console.error('Error in standardizeAddresses:', error.message);
        throw error;
    }
}

// Step 3: Update standardized addresses to database
async function updateStandardizedAddresses(standardizedOrders) {
    try {
        const connection = await mysql.createConnection(dbConfig);
        
        if (standardizedOrders.length > 0) {
            const values = standardizedOrders.map(order => [
                order.MaPX,
                order.DcGiaohang,
                order.District,
                order.Ward
            ]);
            const [result] = await connection.query(
                'INSERT INTO orders_address (id_order, address, district, ward) VALUES ? ' +
                'ON DUPLICATE KEY UPDATE address = VALUES(address), district = VALUES(district), ward = VALUES(ward)',
                [values]
            );
            console.log('Database insert affected rows (orders_address):', result.affectedRows);
        }
        
        await connection.end();
    } catch (error) {
        console.error('Error in updateStandardizedAddresses:', error.message);
        throw error;
    }
}

// Step 4: Group orders by district and ward and return JSON
async function groupOrders() {
    try {
        const connection = await mysql.createConnection(dbConfig);
        
        const [results] = await connection.execute(`
            SELECT district, ward, JSON_ARRAYAGG(
                JSON_OBJECT(
                    'id_order', id_order,
                    'address', address
                )
            ) as orders
            FROM orders_address
            GROUP BY district, ward
        `);
        
        // Parse orders field if needed (JSON_ARRAYAGG returns string in some MySQL versions)
        const parsedResults = results.map(result => ({
            district: result.district,
            ward: result.ward,
            orders: typeof result.orders === 'string' ? JSON.parse(result.orders) : result.orders
        }));
        
        await connection.end();
        console.log('Grouped orders count:', parsedResults.length);
        return parsedResults; // Return JSON-compatible array
    } catch (error) {
        console.error('Error in groupOrders:', error.message);
        throw error;
    }
}

// Main function for full processing
async function main() {
    try {
        console.log('Starting delivery tool...');
        
        // Step 1
        console.log('Step 1: Fetching and saving orders...');
        const orders = await fetchAndSaveOrders();
        console.log('Orders saved:', orders.length);

        // Step 2
        console.log('Step 2: Standardizing addresses...');
        const standardizedOrders = await standardizeAddresses(orders);
        console.log('Standardized orders:', standardizedOrders.length);

        // Step 3
        console.log('Step 3: Updating standardized addresses...');
        await updateStandardizedAddresses(standardizedOrders);
        console.log('Standardized addresses updated');

        // Step 4
        console.log('Step 4: Grouping orders...');
        const groupedOrders = await groupOrders();
        console.log('Grouped orders:', JSON.stringify(groupedOrders, null, 2));
        
        console.log('Delivery tool completed.');
        return groupedOrders; // Return for API response
    } catch (error) {
        console.error('Error in main:', error.message);
        throw error;
    }
}

module.exports = { main, groupOrders };