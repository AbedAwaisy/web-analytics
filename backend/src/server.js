const express = require('express');
const cors = require('cors');
const router = require('./routes/router'); // Import the router for handling getData endpoint
const multer = require('multer');
const session = require("client-sessions");

const app = express();
app.use(cors());
app.use(express.json());
const storage = multer.memoryStorage(); // Store files in memory


const upload = multer({ storage: storage });
app.post('/upload', upload.single('file'), (req, res, next) => {
  next();
});
app.use('/', router);

app.use(
  session({
    cookieName: "session", // the cookie key name
    //secret: process.env.COOKIE_SECRET, // the encryption key
    secret: "template", // the encryption key
    duration: 24 * 60 * 60 * 1000, // expired after 20 sec
    activeDuration: 1000 * 60 * 5, // if expiresIn < activeDuration,
    cookie: {
      httpOnly: false,
    }
    //the session will be extended by activeDuration milliseconds
  })
);











const PORT = 3001;

app.listen(PORT, () => {
  console.log(`Server is listening on port ${PORT}`);
});
