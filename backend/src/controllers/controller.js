const pool = require('../db');
const XLSX = require('xlsx'); // Import XLSX library for parsing Excel files
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const saltRounds = 10;
const secretKey = 'mySecretKeyForJWTs_$2a$12$P5kgkrCnW2zX02dBy6r1Gu';

const dataController = {
  experimentOptions: async (req, res) => {
    console.log('Sort type:', req.params.sortType);
    const sortType = req.params.sortType; // Assuming the sortType is passed as a query parameter

    // Call the stored procedure passing the sortType as an argument
    const sqlQuery = 'CALL GetExperimentOptionsBySortType(?)';
    pool.query(sqlQuery, [sortType], (err, results) => {
        if (err) {
            console.error('Error fetching data:', err);
            res.status(500).json({ message: 'Error fetching data', error: err });
        } else {
            res.json(results[0]);
        }
    });
  },
  sortOptions: async (req, res) => {
    console.log('Fetching sorting options');
    const sqlQuery = 'SELECT * FROM UniqueSortingTypes';
    pool.query(sqlQuery, (err, results) => {
      if (err) {
        console.error('Error fetching data:', err);
        res.status(500).json({ message: 'Error fetching data', error: err });
      } else {
        res.json(results);
      }
    });
  },

  insertUser: async (req, res) => {
    try {
      const { name, email, password, mop } = req.body;
      // Basic validation
     /* let user_details = {
        name: req.body.name,
        email: req.body.email,
        password: req.body.password,
        mop: req.body.mop,

      }*/
      // Replace the following code with your actual database insertion logic
      const sqlQuery = 'INSERT INTO users (name, email, password, mop) VALUES (?, ?, ?, ?)';
      //const hashedPassword = await bcrypt.hash(password, saltRounds);
      pool.query(sqlQuery, [name, email, password, mop], (err, result) => {
        if (err) {
          console.error('Error inserting user:', err);
          res.status(500).json({ message: 'Error inserting user', error: err }); // Provide detailed error message
        } else {
          console.log('User inserted successfully');
          res.status(200).json({ message: 'User inserted successfully' });
        }
      });
    } catch (error) {
      console.error('Error inserting user:', error);
      res.status(500).json({ message: 'Error inserting user', error: error}); // Provide detailed error message
    }

  },




  loginUser: async (req, res) => {
    try {
      const sql = 'SELECT * FROM users WHERE email = ? AND password = ?';
      pool.query(sql, [req.body.email, req.body.password], (err, data) => {
        if (err) {
          console.error('Error querying database:', err);
          return res.json("Error");
        }
        if (data.length > 0) {
          return res.json("Success");
        } else {
          return res.json("Fail");
        }
      });
    } catch (error) {
      console.error('Error logging in:', error);
      res.status(500).json({ message: 'Error logging in. Please try again.' });
    }
  },
  getData: async (req, res) => {
    const sortType = req.params.sortType;
    const experimentType = req.params.experimentType;
    console.log('Sort type:', sortType);
    console.log('Experiment type:', experimentType);
    // Call the stored procedure passing both sortType and experimentType as arguments
    const sqlQuery = 'CALL JoinTables(?, ?)';
    pool.query(sqlQuery, [sortType, experimentType], (err, results) => {
        if (err) {
            console.error('Error fetching data:', err);
            res.status(500).json({ message: 'Error fetching data', error: err });
        } else {
            res.json(results[0]);
        }
    });
  }
}

module.exports = dataController;