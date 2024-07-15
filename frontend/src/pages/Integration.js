import React, { useState, useEffect } from 'react';
import axios from 'axios';
import Modal from 'react-modal';

Modal.setAppElement('#root'); // Ensure this is set for accessibility

const Integration = () => {
  const [file, setFile] = useState(null);
  const [error, setError] = useState('');
  const [dragging, setDragging] = useState(false);
  const [serverResponse, setServerResponse] = useState('');
  const [ws, setWs] = useState(null);
  const [modalIsOpen, setModalIsOpen] = useState(false);
  const [modalMessage, setModalMessage] = useState('');
  const [modalType, setModalType] = useState(''); // 'confirm' or 'prompt'

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
    const wsInstance = new WebSocket("ws://localhost:8000/ws");
    setWs(wsInstance);

    wsInstance.onopen = () => {
      wsInstance.send(file_name);
    };

    wsInstance.onmessage = async (event) => {
      console.log(event.data);
      handleWebSocketMessage(event.data, wsInstance);
    };

    wsInstance.onclose = () => {
      console.log("WebSocket connection closed.");
    };

    wsInstance.onerror = (error) => {
      console.error("WebSocket error:", error);
    };
  };

  const handleWebSocketMessage = async (message, wsInstance) => {
    if (message.toLowerCase().includes("yes/no")) {
      setModalMessage(message);
      setModalType('confirm');
      setModalIsOpen(true);
    } else if (message.toLowerCase().includes("please enter")) {
      setModalMessage(message);
      setModalType('prompt');
      setModalIsOpen(true);
    } else {
      alert(message);
    }
  };

  const handleModalClose = (response) => {
    if (ws) {
      ws.send(response);
    }
    setModalIsOpen(false);
  };

  useEffect(() => {
    // Cleanup WebSocket on component unmount
    return () => {
      if (ws) {
        ws.close();
      }
    };
  }, [ws]);

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
      {serverResponse && <p>{serverResponse}</p>}
      
      <Modal
        isOpen={modalIsOpen}
        onRequestClose={() => handleModalClose('')}
        contentLabel="User Input Modal"
      >
        <h2>{modalMessage}</h2>
        {modalType === 'confirm' && (
          <div>
            <button onClick={() => handleModalClose('Yes')}>Yes</button>
            <button onClick={() => handleModalClose('No')}>No</button>
          </div>
        )}
        {modalType === 'prompt' && (
          <div>
            <input
              type="text"
              onKeyPress={(e) => {
                if (e.key === 'Enter') handleModalClose(e.target.value);
              }}
            />
            <button onClick={() => handleModalClose('')}>Submit</button>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default Integration;
