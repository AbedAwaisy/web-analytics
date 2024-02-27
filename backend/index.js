const express = require('express');
const mysql = require('mysql');
const cors = require('cors'); // Import the cors middleware

const app = express();
app.use(cors());

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

app.get('/data', async (req, res) => {
  // Replace with your actual query
  const sqlQuery = 'SELECT * FROM Persons';
  
  pool.query(sqlQuery, (err, results) => {
    if (err) {
      console.error('Error11 fetching data:', err);
      res.status(500).json({ message: 'Error fetching data', error: err });
    } else {
      res.json(results);
    }
  });
});

const PORT = 3001;

app.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});
