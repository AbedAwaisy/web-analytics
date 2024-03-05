const pool = require('../db');
const XLSX = require('xlsx'); // Import XLSX library for parsing Excel files

const dataController = {
  
  getData: async = (req, res) => {
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
  }, 

  uploadData: async (req, res) => {
    try {
        // Read uploaded file from request body
        const file = req.file.buffer;
        //res.status(200).json({ message: 'File uploaded successfully' });
        // Parse Excel file data
        const workbook = XLSX.read(file, { type: 'buffer' });
        const sheetName = workbook.SheetNames[0]; // Assuming there's only one sheet
        const worksheet = workbook.Sheets[sheetName];
        const data = XLSX.utils.sheet_to_json(worksheet, { header: 1 });

        // Prepare data for database insertion
        const values = data.slice(1); // Skip header row
        const sqlQuery = 'INSERT INTO Persons (PersonID, LastName, FirstName, Address, City) VALUES ?';
        console.log('Data to be uploaded:', values);
        // Insert data into the database
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
}
}

module.exports = dataController;