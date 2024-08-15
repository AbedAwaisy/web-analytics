import React, { useState, useEffect } from 'react';
import './ExportData.css'; 
import { fetchDataFromDB, fetchSortOptionsFromDB, fetchExperimentOptionsFromDB } from '../api/api';

const ExportData = () => {
    const [data, setData] = useState([]);
    const [sortType, setSortType] = useState('');
    const [experimentType, setExperimentType] = useState('');
    const [viewMode, setViewMode] = useState('table'); // Default view mode is 'table'
    const [sortOptions, setSortOptions] = useState([]); // State to hold sorting options
    const [experimentOptions, setExperimentOptions] = useState([]); // State to hold experiment options
    const [filters, setFilters] = useState({}); // State to hold filter values

    const handleFetchData = async () => {
        try {
            const fetchedData = await fetchDataFromDB(sortType, experimentType); // Fetch data from the API
            setData(fetchedData);
        } catch (error) {
            console.error('Error fetching data:', error);
        }
    };

    const fetchSortOptions = async () => {
        try {
            const fetchedSortOptions = await fetchSortOptionsFromDB(); // Fetch sorting options from the API
            setSortOptions(fetchedSortOptions);
            setSortType(fetchedSortOptions[0].SortingType); // Set the default sort type
        } catch (error) {
            console.error('Error fetching sorting options:', error);
        }
    };

    const fetchExperimentOptions = async (sortType) => {
        if (sortType !== '') { 
            try {
                const fetchedExperimentOptions = await fetchExperimentOptionsFromDB(sortType); // Fetch experiment options from the API
                setExperimentOptions(fetchedExperimentOptions);
                setExperimentType(fetchedExperimentOptions[0].ExperimentType); // Set the default experiment type
            } catch (error) {
                console.error('Error fetching experiment options:', error);
            }
        }
    };

    useEffect(() => {
        fetchSortOptions(); // Fetch sorting options when component mounts
    }, []);

    useEffect(() => {
        fetchExperimentOptions(sortType); // Fetch experiment options when sortType changes
    }, [sortType]);

    useEffect(() => {
        // Initialize filters with empty strings
        if (data.length > 0) {
            const initialFilters = Object.keys(data[0]).reduce((acc, key) => ({ ...acc, [key]: '' }), {});
            setFilters(initialFilters);
        }
    }, [data]);

    const handleViewModeToggle = () => {
        setViewMode(prevMode => prevMode === 'table' ? 'graph' : 'table');
    };

    const handleFilterChange = (key, value) => {
        setFilters({ ...filters, [key]: value });
    };

    const filteredData = data.filter(row =>
        Object.keys(filters).every(key => row[key].toString().toLowerCase().includes(filters[key].toLowerCase()))
    );

    return (
        <>
        <div className="container_1">
            <button className="toggle-view-btn" onClick={handleViewModeToggle}>
                {viewMode === 'table' ? 'View Graph' : 'View Table'}
            </button>

            {viewMode === 'table' && (
                <>
                    <div className="dropdown">
                        <label>Sorting Type:</label>
                        <select onChange={(e) => setSortType(e.target.value)} value={sortType} className="select">
                            {sortOptions.map((option, index) => (
                                <option key={index} value={option.SortingType}>
                                    {option.SortingType}
                                </option>
                            ))}
                        </select>
                    </div>

                    <div className="dropdown">
                        <label>Experiment Type:</label>
                        <select onChange={(e) => setExperimentType(e.target.value)} value={experimentType} className="select">
                            {experimentOptions.map((option, index) => (
                                <option key={index} value={option.ExperimentType}>
                                    {option.ExperimentType}
                                </option>
                            ))}
                        </select>
                    </div>

                    <button className="submit-btn" onClick={handleFetchData}>Submit</button>
                </>
            )}
                  <div>
        <a href="#Chatbot" className="chatbot">Go to Chatbot</a>
      </div>
        </div>
        
        <h2 className='title_integration'>Data Table And Graphs</h2>
        
        {viewMode === 'table' && filteredData.length > 0 ? (
                <div className="data-table">
                    <table>
                        <thead>
                            <tr>
                                {Object.keys(filters).map((key, index) => (
                                    <th key={index}>
                                        <input
                                            type="text"
                                            placeholder={`Filter ${key}`}
                                            value={filters[key]}
                                            onChange={(e) => handleFilterChange(key, e.target.value)}
                                            className="filter-input"
                                        />
                                    </th>
                                ))}
                            </tr>
                            <tr>
                                {Object.keys(filteredData[0]).map((header, index) => (
                                    <th key={index}>{header}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {filteredData.map((row, rowIndex) => (
                                <tr key={rowIndex}>
                                    {Object.values(row).map((value, cellIndex) => (
                                        <td key={cellIndex}>{value}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            ) : (viewMode === 'table' &&
                <div className="no-data-placeholder">
                    No data available to display.
                </div>
            )}
                    {viewMode === 'graph' && (
                        <div className="dashboard-container">
                            <iframe
                                src="http://127.0.0.1:8050/"
                                className="dashboard-iframe"
                                title="Dash Application"
                            />
                        </div>
                    )}
                  <h2 id="Chatbot" className='title_chatbot'>Chatbot</h2>
      
                    </>
    );
};

export default ExportData;
