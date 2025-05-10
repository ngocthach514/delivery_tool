require('dotenv').config();
const express = require('express');
const { main, groupOrders } = require('./delivery-tool');

const app = express();
const port = process.env.PORT || 3000;

app.get('/grouped-orders', async (req, res) => {
    try {
        console.time('grouped-orders');
        const groupedOrders = await groupOrders();
        console.timeEnd('grouped-orders');
        res.json(groupedOrders);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/process-orders', async (req, res) => {
    try {
        console.time('process-orders');
        const groupedOrders = await main();
        console.timeEnd('process-orders');
        res.json(groupedOrders);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});