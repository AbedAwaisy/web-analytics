const pool = require('../db');
const XLSX = require('xlsx'); // Import XLSX library for parsing Excel files
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const saltRounds = 10;
const secretKey = 'mySecretKeyForJWTs_$2a$12$P5kgkrCnW2zX02dBy6r1Gu';

const dataController = {
  getData: async (req, res) => {
    const sqlQuery = 'SELECT * FROM Person';
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

      const checkEmailQuery = 'SELECT * FROM users WHERE email = ?';
      pool.query(checkEmailQuery, [email], async (err, results) => {
        if (err) {
          console.error('Error checking email:', err);
          return res.status(500).json({ message: 'Error checking email', error: err });
        }

        if (results.length > 0) {
          console.log('User already exists:', email);
          return res.status(400).json({ message: 'User already exists' });
        }
        const hashedPassword = await bcrypt.hash(password, saltRounds); // Hash the password
        const sqlQuery = 'INSERT INTO users (name, email, password, mop) VALUES (?, ?, ?, ?)';
        pool.query(sqlQuery, [name, email, hashedPassword, mop], (err, result) => {
          if (err) {
            console.error('Error inserting user:', err);
            res.status(500).json({ message: 'Error inserting user', error: err });
          } else {
            console.log('User inserted successfully:', email);
            res.status(200).json({ message: 'User inserted successfully' });
          }
        });
      });
    } catch (error) {
      console.error('Error inserting user:', error);
      res.status(500).json({ message: 'Error inserting user', error: error });
    }
  },

  loginUser: async (req, res) => {
    try {
      const { email, password } = req.body;
      const sql = 'SELECT * FROM users WHERE email = ?';
      pool.query(sql, [email], async (err, data) => {
        if (err) {
          console.error('Error querying database:', err);
          return res.json("Error");
        }
        if (data.length > 0) {
          const match = await bcrypt.compare(password, data[0].password); // Compare hashed passwords
          if (match) {
            req.session.user = email;
            return res.json("Success");
          } else {;
            return res.json("Fail");
          }
        } else {
          return res.json("Fail");
        }
      });
    } catch (error) {
      res.status(500).json({ message: 'Error logging in. Please try again.' });
    }
  },
  uploadData: async (req, res) => {
    try {
      const file = req.file.buffer;
      const workbook = XLSX.read(file, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const worksheet = workbook.Sheets[sheetName];
      const data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });
      const values = data.slice(1);
      const sqlQuery = 'INSERT INTO Persons (PersonID, LastName, FirstName, Address, City) VALUES ?';
      pool.query(sqlQuery, [values], (err, result) => {
        if (err) {
          console.error('Error uploading data:', err);
          res.status(500).json({ message: 'Error uploading data', error: err });
        } else {
          console.log('Data uploaded successfully');
          res.status(200).json({ message: 'Data uploaded successfully' });
        }
      });
    } catch (error) {
      console.error('Error parsing file:', error);
      res.status(400).json({ message: 'Error parsing file', error: error });
    }
  },

  saveNote: async (req, res) => {
    try {
      const user = req.session.user;
      if (!user) {
        console.error('Unauthorized access - No session user');
        return res.status(401).json({ message: 'Unauthorized' });
      }
      const { note } = req.body;
      const insertQuery = 'INSERT INTO notes (user, note) VALUES (?, ?)';
      pool.query(insertQuery, [user, note], (insertErr, insertResult) => {
        if (insertErr) {
          console.error('Error saving note:', insertErr);
          return res.status(500).json({ message: 'Error saving note', error: insertErr });
        } else {
          return res.status(200).json({ message: 'Note saved successfully', id: insertResult.insertId });
        }
      });
    } catch (error) {
      console.error('Error saving note:', error);
      return res.status(500).json({ message: 'Error saving note', error: error });
    }
  },

  getNotes: async (req, res) => {
    try {
      const user = req.session.user;
      if (!user) {
        return res.status(401).json({ message: 'Unauthorized' });
      }
      const sqlQuery = 'SELECT id, note FROM notes WHERE user = ?';
      pool.query(sqlQuery, [user], (err, results) => {
        if (err) {
          console.error('Error fetching notes:', err);
          res.status(500).json({ message: 'Error fetching notes', error: err });
        } else {
          res.json(results);
        }
      });
    } catch (error) {
      console.error('Error fetching notes:', error);
      res.status(500).json({ message: 'Error fetching notes', error: error });
    }
  },

  updateNote: async (req, res) => {
    try {
      const user = req.session.user;
      console.log('Session set:', req.session);
      if (!user) {
        console.error('Unauthorized access - No session user');
        return res.status(401).json({ message: 'Unauthorized' });
      }
      const { id, note } = req.body;
      const updateQuery = 'UPDATE notes SET note = ? WHERE id = ? AND user = ?';
      pool.query(updateQuery, [note, id, user], (updateErr, updateResult) => {
        if (updateErr) {
          console.error('Error updating note:', updateErr);
          return res.status(500).json({ message: 'Error updating note', error: updateErr });
        } else {
          return res.status(200).json({ message: 'Note updated successfully' });
        }
      });
    } catch (error) {
      console.error('Error updating note:', error);
      return res.status(500).json({ message: 'Error updating note', error: error });
    }
  },

  deleteNote: async (req, res) => {
    try {
      const user = req.session.user;
      if (!user) {
        console.error('Unauthorized access - No session user');
        return res.status(401).json({ message: 'Unauthorized' });
      }
      const { id } = req.body;
      const deleteQuery = 'DELETE FROM notes WHERE id = ? AND user = ?';
      pool.query(deleteQuery, [id, user], (deleteErr, deleteResult) => {
        if (deleteErr) {
          console.error('Error deleting note:', deleteErr);
          return res.status(500).json({ message: 'Error deleting note', error: deleteErr });
        } else {
          return res.status(200).json({ message: 'Note deleted successfully' });
        }
      });
    } catch (error) {
      console.error('Error deleting note:', error);
      return res.status(500).json({ message: 'Error deleting note', error: error });
    }
  },

  protectedResource: (req, res) => {
    if (req.session && req.session.user) {
      res.status(200).json({ message: 'Access granted' });
    } else {
      res.status(401).json({ message: 'Unauthorized' });
    }
  },

  logoutUser: async (req, res) => {
    try {
      req.session.reset(); 
      res.status(200).json({ message: 'Logged out successfully' });
    } catch (error) {
      console.error('Error logging out:', error);
      res.status(500).json({ message: 'Error logging out', error: error });
    }
  }
};

module.exports = dataController;
