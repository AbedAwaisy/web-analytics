const express = require('express');
const cors = require('cors');
const router = require('./routes/router'); // Import the router for handling getData endpoint
const multer = require('multer');

const app = express();
app.use(cors());
const storage = multer.memoryStorage(); // Store files in memory


const upload = multer({ storage: storage });
app.post('/upload', upload.single('file'), (req, res, next) => {
  next();
});
app.use('/', router);


const PORT = 3001;

app.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});
