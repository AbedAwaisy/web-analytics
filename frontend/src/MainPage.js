import React, { useState } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Header from './components/Header/Header';
import Login from './components/Login/Login';
import Home from './pages/Home';
import ExportData from './pages/ExportData';
import Integration from './pages/Integration';
import Register from './components/Register/Register';
import ForgotPassword from './components/ForgotPassword/ForgotPassword';

const MainPage = () => {
  const [ setIsLoggedIn] = useState(false);

  const handleLogin = () => {
    // Implement your login logic here
    setIsLoggedIn(true);
  };

  return (
    <Router>
      <Routes>     
        <Route path="/login" element={<Login onLogin={() => { handleLogin(); return <Navigate to="/home" />; }} />} />
        <Route path="/home" element={<><Header /><Home /></>}/>
        <Route path="/exportdata" element={<><Header /><ExportData /></>}/>
        <Route path="/integration" element={<><Header /><Integration /></>}/>
        <Route path="/register" element={<Register />} />
        <Route path="/forgotpassword" element={<ForgotPassword />} />
        <Route path="/" element={<Navigate to="/login" />} />
      </Routes>
    </Router>
  );
};

export default MainPage;
