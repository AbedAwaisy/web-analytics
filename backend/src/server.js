const express = require('express');
const cors = require('cors');
const multer = require('multer');
const session = require('client-sessions');
const router = require('./routes/router'); // Import the router for handling getData endpoint

const app = express();
app.use(cors({
  origin: 'http://localhost:3000', // Your frontend URL
  credentials: true // Allow credentials (cookies, authorization headers, TLS client certificates)
}));
app.use(express.json());

app.use(
  session({
    cookieName: "session", // the cookie key name
    secret: "template", // the encryption key
    duration: 24 * 60 * 60 * 1000, // 24 hours
    activeDuration: 1000 * 60 * 5, // if expiresIn < activeDuration, the session will be extended by activeDuration milliseconds
    cookie: {
      httpOnly: false,
      secure: false // Ensure this is false if you're testing locally without HTTPS
    }
  })
);

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