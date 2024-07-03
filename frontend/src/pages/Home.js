import React from 'react';
import { Link } from 'react-router-dom';
import './Home.css';
import Header from '../components/Header/Header';

const Home = () => {
  return ( 
    <>
      <div>
        <a href="#about-us" className="scroll-about-us">About Project</a>
      </div>
      <h2 className='title'>Welcome to Tomato Data Project!</h2>
      <div className="home-container">
        <div className="mop-container">
          <h2 className="mop-title">Desert - Negev Ramat MOP</h2>
          <p className="mop-description">
            Description: The MOP Ramat Negev is a center dedicated to research and development in desert agriculture. It focuses on developing cultivation methods, irrigation, fertilization, and processing of agricultural crops suitable for the unique ecological conditions of the Negev region.
          </p>
          <a href="https://www.moprn.org/cgi-webaxy/item?318" target="_blank" rel="noopener noreferrer" className="learn-more-link">Learn more</a>
        </div>

        <div className="mop-container">
          <h2 className="mop-title">Darom MOP</h2>
          <p className="mop-description">
            Description: MOP Darom is the first regional R&D center established in Israel. Initially focusing on agricultural development in northern Sinai, it shifted its focus to the western and northern Negev regions after changes following the peace agreement with Egypt.
          </p>
          <a href="http://mopdarom.org.il/%d7%93%d7%95%d7%97%d7%95%d7%aa-%d7%9e%d7%a7%d7%a6%d7%95%d7%a2%d7%99%d7%99%d7%9d/" target="_blank" rel="noopener noreferrer" className="learn-more-link">Learn more</a>
        </div>

        <div className="mop-container">
          <h2 className="mop-title">Central and Northern Arava</h2>
          <p className="mop-description">
            Description: R&D in the Central and Northern Arava focuses on serving the development needs of settlements along Israel's borders in the Negev and Arava regions. It conducts research in various agricultural fields and collaborates with academic institutions, government bodies, and regional councils.
          </p>
          <a href="http://agri.arava.co.il/research" target="_blank" rel="noopener noreferrer" className="learn-more-link">Learn more</a>
        </div>
      </div>
      <h2 className='title'>About Project</h2>
      <div id="about-us" className="about-us-section">
        <p>
         there are three pages....
        </p> 
      </div>
    </>
  );
};

export default Home;
