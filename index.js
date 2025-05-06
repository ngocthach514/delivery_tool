require('dotenv').config();
const { main } = require('./src/delivery-tool');

main().catch(console.error);