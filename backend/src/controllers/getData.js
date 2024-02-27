const pool = require('../db');

const getData = (req, res) => {
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
};

module.exports = {
  getData
};
