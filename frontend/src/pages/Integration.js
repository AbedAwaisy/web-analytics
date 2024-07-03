import React, { useState } from 'react';
import { uploadFile } from '../api/api.js';
import './Integration.css'; // Import the CSS file

const Integration = () => {
  const [file, setFile] = useState(null);
  const [error, setError] = useState('');
  const [dragging, setDragging] = useState(false);

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
    const fileExtension = file.name.split('.').pop().toLowerCase();
    if (!(fileExtension === 'csv' || fileExtension === 'xlsx')) {
      setError(`Unsupported file type: ${fileExtension ? `.${fileExtension}` : 'Unknown'}. Supported file types are .csv and .xlsx.`);
      return;
    }
    uploadFile(file, setError);
  };

  return (
    <div>
      <h2 className='title'>Choose Tomato Data File</h2>
      <form onSubmit={handleFormSubmit}className="uploadForm">
        <input type="file" onChange={handleFileChange} className="fileInputControl" />
        <div
          onDrop={handleDrop}
          onDragOver={(event) => {
            event.preventDefault();
            event.stopPropagation();
            setDragging(true);
          }}
          onDragEnter={() => setDragging(true)}
          onDragLeave={() => setDragging(false)}
          className={`fileInput ${dragging ? 'dragging' : ''}`}
        >
          <p>{file ? `Selected file: ${file.name}` : 'Drag and drop file here'}</p>
        </div>
        {error && <p className="errorMessage">{error}</p>}
        <button type="submit" className="uploadButton">Upload</button>
      </form>
    </div>
  );
};

export default Integration;
