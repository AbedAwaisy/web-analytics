import React, { useState } from 'react';
import {uploadFile} from '../api/api.js';
import axios from 'axios';

const Integration = () => {
  const [file, setFile] = useState(null);
  const [error, setError] = useState('');
  const [dragging, setDragging] = useState(false);
  const [serverResponse, setServerResponse] = useState('');


  const handleFileChange = (event) => {
    const selectedFile = event.target.files[0];
    setFile(selectedFile);
    setError(''); // Reset error message
  };

  const handleDrop = (event) => {
    event.preventDefault();
    event.stopPropagation();
    const selectedFile = event.dataTransfer.files[0];
    setFile(selectedFile);
    setDragging(false);
    setError(''); // Reset error message
  };

  const handleFormSubmit = async (event) => {
    event.preventDefault();
    if (!file) {
      setError('Please select a file.');
      return;
    }
    const formData = new FormData();
    formData.append('file', file);
    
    try {
      const response = await axios.post('http://localhost:8000/upload', formData);
      if (response.data.status === "Columns check required") {
        initiateWebSocketConnection(response.data.file_name);
      } else {
        setServerResponse(response.data.status);
      }
    } catch (error) {
      console.error('Error uploading file:', error);
      setError('Error uploading file. Please try again.');
    }
  };

  const initiateWebSocketConnection = (file_name) => {
    const ws = new WebSocket("ws://localhost:8000/ws");
    ws.onopen = () => {
      ws.send(file_name);
    };
    ws.onmessage = (event) => {
      console.log(event.data);
      if (event.data.includes("Does this file have an Experiment column?")) {
          const hasExperiment = window.confirm(event.data) ? "Yes" : "No";
          ws.send(hasExperiment);
      } else if (event.data.includes("Please enter the Experiment column name:")) {
          const experimentColumnName = prompt(event.data);
          ws.send(experimentColumnName);
      } else if (event.data.includes("Proceed with integration?")) {
          const proceed = window.confirm(event.data) ? "Yes" : "No";
          ws.send(proceed);
      } else {
          alert(event.data); // Show other messages as alerts
      }
  };

  ws.onclose = () => {
      console.log("WebSocket connection closed.");
  };

  ws.onerror = (error) => {
      console.error("WebSocket error:", error);
  };
};

  return (
    <div>
      <h2>Integration</h2>
      <form onSubmit={handleFormSubmit}>
        <input type="file" onChange={handleFileChange} />
        <div
          onDrop={handleDrop}
          onDragOver={(event) => {
            event.preventDefault();
            event.stopPropagation();
            setDragging(true);
          }}
          onDragEnter={() => setDragging(true)}
          onDragLeave={() => setDragging(false)}
          style={{
            border: `2px solid ${dragging ? 'blue' : 'black'}`,
            backgroundColor: dragging ? 'lightblue' : 'white',
            padding: '20px',
            marginTop: '20px',
            borderRadius: '10px',
          }}
        >
          <p>{file ? `Selected file: ${file.name}` : 'Drag and drop file here'}</p>
        </div>
        {error && <p style={{ color: 'red' }}>{error}</p>}
        <button type="submit">Upload</button>
      </form>
      {serverResponse && <p>{serverResponse}</p>}
    </div>
  );
};

export default Integration;