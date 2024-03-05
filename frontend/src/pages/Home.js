import React from 'react';
import { Link } from 'react-router-dom';
import './Home.css';

const Home = () => {
  return (
    <div className="home-container">
      <div className="mop-container">
        <h2 className="mop-title">Desert - Negev Ramat MOP</h2>
        <p className="mop-description">
          Description: The MOP Ramat Negev is a center dedicated to research and development in desert agriculture. It focuses on developing cultivation methods, irrigation, fertilization, and processing of agricultural crops suitable for the unique ecological conditions of the Negev region.
        </p>
        {/* Add other information as needed */}
        <Link to="/mop/ramat-negev">Learn more</Link>
      </div>

      <div className="mop-container">
        <h2 className="mop-title">Darom MOP</h2>
        <p className="mop-description">
          Description: MOP Darom is the first regional R&D center established in Israel. Initially focusing on agricultural development in northern Sinai, it shifted its focus to the western and northern Negev regions after changes following the peace agreement with Egypt.
        </p>
        {/* Add other information as needed */}
        <Link to="/mop/darom">Learn more</Link>
      </div>

      <div className="mop-container">
        <h2 className="mop-title">Development and Research (R&D) in the Central and Northern Arava</h2>
        <p className="mop-description">
          Description: R&D in the Central and Northern Arava focuses on serving the development needs of settlements along Israel's borders in the Negev and Arava regions. It conducts research in various agricultural fields and collaborates with academic institutions, government bodies, and regional councils.
        </p>
        {/* Add other information as needed */}
        <Link to="/mop/central-northern-arava">Learn more</Link>
      </div>
    </div>
  );
};

export default Home;
