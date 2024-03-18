import React, { useState, useEffect } from 'react';
import './ExportData.css'; 
import { fetchDataFromDB, fetchSortOptionsFromDB, fetchExperimentOptionsFromDB } from '../api/api';
import { Chart } from "react-google-charts";

const ExportData = () => {
    const [data, setData] = useState([]);
    const [sortType, setSortType] = useState('');
    const [experimentType, setExperimentType] = useState('');
    const [googleChartData, setGoogleChartData] = useState([]);
    const [controls, setControls] = useState([]);
    const [viewMode, setViewMode] = useState('table'); // Default view mode is 'table'
    const [sortOptions, setSortOptions] = useState([]); // State to hold sorting options
    const [experimentOptions, setExperimentOptions] = useState([]); // State to hold experiment options

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
                console.error('Error fetching sorting options:', error);
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
        if (data.length > 0) {
            // Dynamically extract keys from the first item to use as headers
            const headers = Object.keys(data[0]);
            // Map the data to the format required by Google Charts
            const googleData = data.map(item => headers.map(header => item[header]));
            // Add the headers row at the beginning
            googleData.unshift(headers);
            setGoogleChartData(googleData);
    
            // Define filter controls based on the headers
            const controlArray = headers.map((header, index) => ({
                controlType: 'StringFilter',
                options: {
                    filterColumnIndex: index,
                    matchType: 'any',
                    ui: {
                        label: header,
                    },
                },
            }));
            setControls(controlArray);
        } else {
            // Reset googleChartData and controls if there is no data
            setGoogleChartData([]);
            setControls([]);
        }
    }, [data]);

    const handleViewModeToggle = () => {
        setViewMode(prevMode => prevMode === 'table' ? 'graph' : 'table');
    };

    return (
        console.log('rendred:'),
        <div className="container">
            <div className="dropdown">
            <label>Sorting Type:</label>
            <select onChange={(e) => setSortType(e.target.value)} value={sortType} className="select">
                {sortOptions.map((option, index) => (
                    <option key={index} value={option.SortingType}> {/* Accessing SortingType key */}
                        {option.SortingType} {/* Display SortingType value */}
                    </option>
                    ))}
                </select>
            </div>


            <div className="dropdown">
                <label>Experiment Type:</label>
                <select onChange={(e) => setExperimentType(e.target.value)} value={experimentType} className="select">
                    {experimentOptions && experimentOptions.map((option, index) => (
                        <option key={index} value={option.ExperimentType}>
                            {option.ExperimentType}
                        </option>
                    ))}
                </select>
            </div>

            <button className="submit-btn" onClick={handleFetchData}>Submit</button>

            <button className="toggle-view-btn" onClick={handleViewModeToggle}>
                {viewMode === 'table' ? 'View Graph' : 'View Table'}
            </button>
            {viewMode === 'table' && googleChartData.length > 1 ? (
                //here
                <div className="google-chart">
                    <Chart
                        chartType="Table"
                        data={googleChartData}
                        width="100%"
                        height="400px"
                        chartPackages={['controls']}
                        controls={controls}
                    />
                </div>
            ) : (viewMode === 'table' &&
                <div className="no-data-placeholder">
                    No data available to display.
                </div>
            )}


{viewMode === 'graph' && googleChartData.length > 1 ? (
    <div className="google-chart">
        <Chart
            chartType="Bar"
            loader={<div>Loading Chart</div>}
            data={googleChartData}
            width="100%"
            height="400px"
            options={{
                chart: {
                    title: 'Person Data',
                },
            }}
        />
    </div>
) : (viewMode === 'graph' &&
    <div className="no-data-placeholder">
        No data available to display.
    </div>
)}
        </div>
    );
};

export default ExportData;
