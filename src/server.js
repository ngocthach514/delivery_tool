require("dotenv").config();
const express = require("express");
const http = require("http");
const socketIo = require("socket.io");
const {
  main,
  groupOrders,
  updatePriorityStatus,
} = require("./delivery-tool");

const app = express();
const server = http.createServer(app);
const io = socketIo(server);
const port = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static("public")); // Phục vụ file tĩnh từ thư mục public

// Lưu trữ client để phát tín hiệu
io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);
  socket.on("disconnect", () => {
    console.log("Client disconnected:", socket.id);
  });
});

// Middleware để truyền io vào hàm updatePriorityStatus
app.use((req, res, next) => {
  req.io = io;
  next();
});

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
    const groupedOrders = await main(page, req.io);

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
