import axios from 'axios';

export const uploadFile = async (file, setError) => {
  try {
    const formData = new FormData();
    formData.append('file', file);
    console.log('File object:', file);
    console.log('FormData:', formData);
    
    const response = await axios.post('http://localhost:3001/upload', formData);
    console.log('File uploaded successfully');
    setError('File uploaded successfully');
    return response.data; // Return the response data
  } catch (error) {
    console.error('Error uploading file:', error);
    setError('Error uploading file. Please try again.');
    return null;
  }
};


export const fetchDataFromDB = async () => {
      try {
        const response = await axios.get('http://localhost:3001/data');
        return response.data;
      } catch (error) {
        console.error('Error fetching data:', error);
      }
    };