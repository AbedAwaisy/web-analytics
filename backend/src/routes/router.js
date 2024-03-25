const express = require('express');
const router = express.Router();

// Controller
const dataController = require('../controllers/controller');
// Define routes
router.get('/data', dataController.getData);
router.post('/upload', dataController.uploadData);
router.post('/register',dataController.insertUser);
router.post('/login', dataController.loginUser);
module.exports = router;