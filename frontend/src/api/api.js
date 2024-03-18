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


export const fetchDataFromDB = async (sortType, experimentType) => {
      try {
        console.log('sortType:', sortType);
        console.log('experimentTYPESSS:', experimentType);
        const response = await axios.get(`http://localhost:3001/data/${sortType}/${experimentType}`);
        return response.data;
      } catch (error) {
        console.error('Error fetching data:', error);
      }
    };

export const fetchSortOptionsFromDB = async () => {
      try {
        const response = await axios.get('http://localhost:3001/sortOptions');
        return response.data;
      } catch (error) {
        console.error('Error fetching sorting options:', error);
      }
    }
    export const fetchExperimentOptionsFromDB = async (sortType) => {
      try {
        console.log('sortType:', sortType);
          const response = await axios.get(`http://localhost:3001/experimentOptions/${sortType}`);
          console.log('Experiment options:', response.data);
          return response.data;
      } catch (error) {
          console.error('Error fetching experiment options:', error);
      }
  };
  