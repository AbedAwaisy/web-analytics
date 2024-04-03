import axios from 'axios';

export const uploadFile = async (file, setError) => {
  try {
    const formData = new FormData();
    formData.append('file', file);
    console.log('File object:', file);
    console.log('FormData:', formData);
    
    const response = await axios.post('http://localhost:8000/upload', formData);
    console.log('File uploaded successfully');
    setError('File uploaded successfully');
    console.log(response.data);
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
  


    
export const insertUserToDB = async (userData, setError) => {
  try {
     const response = await axios.post('http://localhost:3001/register', userData);
    // Check if the user was inserted successfully
    if (response) {
      console.log('User inserted successfully:', response.data);
      setError('User iserted successfully');
      return response.data; // Return the inserted user data
    } else {
      console.error('Eerror inserting user:', response.data.error);
      setError('Error inserting user. Please try again.');
      return null;
    }
  } catch (error) {
    console.error('Eerror inserting user:', error);
    setError('Error inserting user. Please try again.');
    return null;
  }
};

export const loginUser = async (loginData, setError) => {
  try {
    const response = await axios.post('http://localhost:3001/login', loginData);

    if (response ) {
      console.log('Login successful:', response.data);
      localStorage.setItem('token', response.data.token); // Store the token in local storage
      setError('Login successful');
      return response.data; // Return the login data
    } else {
      console.error('Login failed:', response.data.message);
      setError('Login failed. Please check your credentials.');
      return null;
    }
  } catch (error) {
    console.error('Error logging in:', error);
    setError('Error logging in. Please try again.');
    return null;
  }
};