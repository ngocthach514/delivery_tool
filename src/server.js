require('dotenv').config();
const express = require('express');
const { main, groupOrders } = require('./delivery-tool');

const app = express();
const port = process.env.PORT || 3000;

// Middleware để parse JSON (nếu cần gửi body trong tương lai)
app.use(express.json());

// Endpoint để lấy danh sách đơn hàng phân trang
app.get('/grouped-orders', async (req, res) => {
  try {
    console.time('grouped-orders');
    
    // Lấy tham số page từ query parameter
    const page = parseInt(req.query.page) || 1;
    
    // Kiểm tra page hợp lệ
    if (isNaN(page) || page < 1) {
      return res.status(400).json({ error: "Page phải là số nguyên dương" });
    }

    console.log(`Gọi groupOrders với page: ${page}`);
    const groupedOrders = await groupOrders(page);
    
    console.timeEnd('grouped-orders');
    res.status(200).json(groupedOrders);
  } catch (error) {
    console.error("Lỗi trong /grouped-orders:", error.message, error.stack);
    res.status(500).json({ error: "Lỗi server", details: error.message });
  }
});

// Endpoint để chạy toàn bộ quy trình
app.get('/process-orders', async (req, res) => {
  try {
    console.time('process-orders');
    
    // Lấy tham số page từ query parameter
    const page = parseInt(req.query.page) || 1;
    
    if (isNaN(page) || page < 1) {
      return res.status(400).json({ error: "Page phải là số nguyên dương" });
    }

    console.log(`Gọi main với page: ${page}`);
    const groupedOrders = await main(page);
    
    console.timeEnd('process-orders');
    res.status(200).json(groupedOrders);
  } catch (error) {
    console.error("Lỗi trong /process-orders:", error.message, error.stack);
    res.status(500).json({ error: "Lỗi server", details: error.message });
  }
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});