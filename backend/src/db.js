const mysql = require('mysql');

// Set up your connection information
const dbConfig = {
  host: '34.165.192.24',
  user: 'root',
  port: 3306,
  password: 'Q4P.M+0t>u$>As+0',
  database: 'Example'
};

// Create a MySQL pool
const pool = mysql.createPool(dbConfig);


module.exports = pool;
