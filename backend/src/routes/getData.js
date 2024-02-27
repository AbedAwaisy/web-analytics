const express = require('express');
const dataController = require('../controllers/getData');

const router = express.Router();

router.get('/', dataController.getData);

module.exports = router;
