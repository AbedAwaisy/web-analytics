import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { logoutUser } from '../../api/api'; // Adjust the path as needed
import './Header.css';

const Header = () => {
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const handleLogout = async () => {
    const response = await logoutUser(setError);
    if (response) {
      navigate('/login');
    }
  };

  const handleSelectChange = (event) => {
    if (event.target.value === 'logout') {
      handleLogout();
    }
  };
  return (
    <header className="header">
      <div className="brand">
        <img src="/images/logo.png" alt="Tomato" className="logo-image" />
        Tomato IntelliGrow
      </div>
      <nav>
        <ul>
          <li>
            <Link to="/home">Home</Link>
          </li>
          <li>
            <Link to="/exportdata">Graphs</Link>
          </li>
          <li>
            <Link to="/integration">Integration</Link>
          </li>
          <li>
            <Link to="/mynotes">Notes</Link>
          </li>
          <li>
          <div className="logout-select">
          <select onChange={handleSelectChange} defaultValue="">
          <option value="" disabled>LogOut</option>
          <option value="logout">Logout</option>
          </select>
         </div>
          </li>
        </ul>
      </nav>

      {error && <p className="error">{error}</p>}
    </header>
  );
};

export default Header;
