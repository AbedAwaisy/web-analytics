const express = require('express');
const router = express.Router();

// Controller
const dataController = require('../controllers/controller');
// Define routes
router.get('/data/:sortType/:experimentType', dataController.getData);
router.get('/sortOptions', dataController.sortOptions);
router.get('/experimentOptions/:sortType', dataController.experimentOptions);
//router.post('/upload', dataController.uploadData);
router.post('/register',dataController.insertUser);
router.post('/login', dataController.loginUser);
module.exports = router;