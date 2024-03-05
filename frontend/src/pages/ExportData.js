import React, { useState } from 'react';
import './ExportData.css'; 
import { fetchDataFromDB } from '../api/api';

const ExportData = () => {
    const [data, setData] = useState([]); // This will be an array of objects
    const [sortType, setSortType] = useState('');
    const [experimentType, setExperimentType] = useState('');

    const sortOptions = ['Option 1', 'Option 2', 'Option 3'];
    const experimentOptions = ['Experiment 1', 'Experiment 2', 'Experiment 3'];

    const handleFetchData = async () => {
        try {
            const fetchedData = await fetchDataFromDB();
            setData(fetchedData);
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    };

    return (
        <div className="container">
          <div className="dropdown">
            <label>Sorting Type:</label>
            <select onChange={(e) => setSortType(e.target.value)} value={sortType} className="select">
              {sortOptions.map((option, index) => (
                <option key={index} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </div>
          
          <div className="dropdown">
            <label>Experiment Type:</label>
            <select onChange={(e) => setExperimentType(e.target.value)} value={experimentType} className="select">
              {experimentOptions.map((option, index) => (
                <option key={index} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </div>
          
          <button className="submit-btn" onClick={handleFetchData}>Submit</button>
          
          {data.length > 0 && (
            <div className="data-table">
              <table>
                <thead>
                  <tr>
                    <th>Person ID</th>
                    <th>Last Name</th>
                    <th>First Name</th>
                    <th>Address</th>
                    <th>City</th>
                  </tr>
                </thead>
                <tbody>
                  {data.map((item, index) => (
                    <tr key={index}>
                      <td>{item.PersonID}</td>
                      <td>{item.LastName}</td>
                      <td>{item.FirstName}</td>
                      <td>{item.Address}</td>
                      <td>{item.City}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
    );
};

export default ExportData;
