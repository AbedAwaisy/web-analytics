import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'; // Import BrowserRouter, Routes, Route, and Link
import Header from './components/Header/Header';
import Home from './pages/Home';
import ExportData from './pages/ExportData';
import Integration from './pages/Integration';


const MainPage = () => {
  return (
    <Router>
      <Header />
      <div>
        <Routes>
          <Route path="/home" element={<Home />} />
          <Route path="/exportdata" element={<ExportData />} />
          <Route path="/integration" element={<Integration />} />
        </Routes>
      </div>
    </Router>
  );
};

export default MainPage;
