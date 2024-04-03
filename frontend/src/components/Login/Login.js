import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import './Login.css'; // Import the CSS module
import { loginUser } from '../../api/api';

const Login = ({ onLogin }) => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [errors, setErrors] = useState({ email: '', password: '' }); // State for validation errors
  const navigate = useNavigate(); // Hook to navigate programmatically

  const validateEmail = (input) => {
    // Regular expression for basic email validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(input);
  };

  const handleLogin = async () => {
    setErrors({ email: '', password: '' });

    let hasErrors = false;

    if (!email) {
      setErrors((prevErrors) => ({ ...prevErrors, email: 'Email is required' }));
      hasErrors = true;
    } else if (!validateEmail(email)) {
      setErrors((prevErrors) => ({ ...prevErrors, email: 'Invalid email format' }));
      hasErrors = true;
    }

    if (!password) {
      setErrors((prevErrors) => ({ ...prevErrors, password: 'Password is required' }));
      hasErrors = true;
    } else if (password.length < 8) {
      setErrors((prevErrors) => ({ ...prevErrors, password: 'Password should be at least 8 characters' }));
      hasErrors = true;
    }

    if (hasErrors) {
      return;
    }

    // Make a login request to the backend
     const loginData = { email, password };
     const response = await loginUser(loginData, setErrors);

     if (response==="Success") {
         navigate('/home');

      }
      else{
        alert("No user existed");
      }

  };
  return (
    <div className='stylescontainer'>
      <h2 className='stylestitle'>Login</h2>
      <label className='styleslabel'>Email:</label>
      <input
        className='stylesinput'
        type="email"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
      />
      {errors.email && <p className='styleserror'>{errors.email}</p>}

      <label className='styleslabel'>Password:</label>
      <input
        className='stylesinput'
        type="password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
      />
      {errors.password && <p className='styleserror'>{errors.password}</p>}

      <button className='stylesbutton' onClick={handleLogin}>
        Login
      </button>

      {/* Link to navigate to the Forgot Password page */}
      <p className='styleslink'>
        <Link to="/forgotpassword">Forgot your password?</Link>
      </p>

      {/* Link to navigate to the Register page */}
      <p className='styleslink'>
        Don't have an account? <Link to="/register">Register here</Link>.
      </p>
    </div>
  );
};

export default Login;
