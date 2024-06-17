const mysql = require('mysql');
require('dotenv').config({path: '/home/corazon/Desktop/web-analytics/.env'}); // Load environment variables from the root .env file


const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  port: process.env.DB_PORT,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE
};

const pool = mysql.createPool(dbConfig);

module.exports = pool;
