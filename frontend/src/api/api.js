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
    const response = await axios.post('http://localhost:3001/register', userData, { withCredentials: true });
    return response.data;
  } catch (error) {
    console.error('Error inserting user:', error);
    setError('Error inserting user. Please try again.');
    if (error.response && error.response.data) {
      return error.response.data;
    }
    return null;
  }
};

export const loginUser = async (loginData, setError) => {
  try {
    const response = await axios.post('http://localhost:3001/login', loginData, { withCredentials: true });

    if (response.data === "Success") {
      console.log('Login successful:', response.data);
      setError('Login successful');
      return response.data;
    } 
    else {
      console.error('Login failed:', response.data);
      setError('Login failed. Please check your credentials.');
      return null;
    }
  } catch (error) {
    console.error('Error logging in:', error);
    setError('Error logging in. Please try again.');
    return null;
  }
};

export const fetchNotes = async (setError) => {
  try {
    const response = await axios.get('http://localhost:3001/notes', { withCredentials: true });
    return response.data;
  } catch (error) {
    console.error('Error fetching notes:', error);
    setError('Error fetching notes. Please try again.');
    return null;
  }
};

export const saveNote = async (note, setError) => {
  try {
    const response = await axios.post('http://localhost:3001/save-note', { note }, { withCredentials: true });
    if (response.status === 200) {
      setError('Note saved successfully');
    }
    return response.data;
  } catch (error) {
    console.error('Error saving note:', error);
    setError('Error saving note. Please try again.');
    return null;
  }
};

export const updateNote = async (id, note, setError) => {
  try {
    const response = await axios.put('http://localhost:3001/update-note', { id, note }, { withCredentials: true });
    if (response.status === 200) {
      setError('Note updated successfully');
    }
    return response.data;
  } catch (error) {
    console.error('Error updating note:', error);
    setError('Error updating note. Please try again.');
    return null;
  }
};
export const deleteNote = async (id, setError) => {
  try {
    const response = await axios.delete('http://localhost:3001/delete-note', { data: { id }, withCredentials: true });
    if (response.status === 200) {
      setError('Note deleted successfully');
    }
    return response.data;
  } catch (error) {
    console.error('Error deleting note:', error);
    setError('Error deleting note. Please try again.');
    return null;
  }
};

export const logoutUser = async (setError) => {
  try {
    const response = await axios.post('http://localhost:3001/logout');
    if (response.status === 200) {
      setError('Logged out successfully');
    }
    return response.data;
  } catch (error) {
    console.error('Error logging out:', error);
    setError('Error logging out. Please try again.');
    return null;
  }
};