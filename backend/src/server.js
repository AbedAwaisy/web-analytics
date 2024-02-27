const express = require('express');
const cors = require('cors');
const pool = require('./db'); // Import the database connection from db.js

const app = express();
app.use(cors());

app.get('/data', async (req, res) => {
  // Replace with your actual query
  const sqlQuery = 'SELECT * FROM Persons';

  pool.query(sqlQuery, (err, results) => {
    if (err) {
      console.error('Error fetching data:', err);
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
