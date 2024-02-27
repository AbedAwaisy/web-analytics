import React, { useState } from 'react';

const MainPage = () => {
  const [data, setData] = useState([]);

  const fetchDataFromDB = async () => {
    try {
      const response = await fetch('http://localhost:3001/data'); 
      const result = await response.json();
      console.log(result);
      setData(result);
    } catch (error) {
      console.error('Error fetching data:', error);
    }
  };

  return (
    <div>
      <button onClick={fetchDataFromDB}>Get Data</button>
      <div>
        {data.map((item, index) => (
          <div key={index}>{JSON.stringify(item)}</div> // Adjust rendering as needed
        ))}
      </div>
    </div>
  );
};

export default MainPage;
