const mysql = require('mysql');

// Set up your connection information
const dbConfig = {
  host: 'my_ip_address',
  user: 'root',
  port: 3306,
  password: 'my_pw',
  database: 'Example'
};

// Create a MySQL pool
const pool = mysql.createPool(dbConfig);


module.exports = pool;
