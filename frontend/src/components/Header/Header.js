import React from 'react';
import { Link } from 'react-router-dom';
import './Header.css';

const Header = () => {
  return (
    <header className="header">
      <div className="brand">
        <img src="/images/tomato.png" alt="Tomato" className="tomato-image" />
        Tomato Data
      </div>
      <nav>
        <ul>
          <li>
            <Link to="/home">Home</Link>
          </li>
          <li>
            <Link to="/exportdata">ExportData</Link>
          </li>
          <li>
            <Link to="/integration">Integration</Link>
          </li>
        </ul>
      </nav>
    </header>
  );
};

export default Header;
