import React, { useState, useEffect } from 'react';
import './ExportData.css'; 
import { fetchDataFromDB } from '../api/api';
import { Chart } from "react-google-charts";

const ExportData = () => {
    const [data, setData] = useState([]);
    const [sortType, setSortType] = useState('');
    const [experimentType, setExperimentType] = useState('');
    const [googleChartData, setGoogleChartData] = useState([]);
    const [controls, setControls] = useState([]);
    const [viewMode, setViewMode] = useState('table'); // Default view mode is 'table'

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

    useEffect(() => {
        // Convert fetched data to the format required by Google Charts
        const googleData = data.map(item => [item.PersonID, item.LastName, item.FirstName, item.Address, item.City]);
        // Add header row
        googleData.unshift(['Person ID', 'Last Name', 'First Name', 'Address', 'City']);
        setGoogleChartData(googleData);

        // Define filter controls
        const controlArray = googleData[0].map((_, index) => ({
            controlType: 'StringFilter',
            options: {
                filterColumnIndex: index,
                matchType: 'any',
                ui: {
                    label: googleData[0][index],
                },
            },
        }));
        setControls(controlArray);
    }, [data]);

    const handleViewModeToggle = () => {
        setViewMode(prevMode => prevMode === 'table' ? 'graph' : 'table');
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
