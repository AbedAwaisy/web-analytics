import React, { useState } from 'react';
import { Link,useNavigate} from 'react-router-dom';
import './Register.css';
import { insertUserToDB } from '../../api/api'; // Adjust the path based on the actual location of api.js

const Register = () => {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [mop, setMop] = useState('');
  const navigate = useNavigate();
  const [errors, setErrors] = useState({
    name: '',
    email: '',
    password: '',
    mop: '',
  });

  const validateEmail = (input) => {
    // Regular expression for basic email validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

    return emailRegex.test(input);
  };

  const handleRegister =async () => {
    // Clear previous errors
    setErrors({
      name: '',
      email: '',
      password: '',
      mop: '',
    });

    let hasErrors = false;

    // Basic validation
    if (!name) {
      setErrors((prevErrors) => ({ ...prevErrors, name: 'Name is required' }));
      hasErrors = true;
    }

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

    if (!mop) {
      setErrors((prevErrors) => ({ ...prevErrors, mop: 'MopName is required' }));
      hasErrors = true;
    }
    if (hasErrors) {
      return;
    }
    const userData = { name, email, password, mop };

    const result = await insertUserToDB(userData, setErrors);
    console.log(result)


    if (result) {
        console.log('Registration successful:', result);
        alert("Register Success");
      } else {
        console.error('Registration failed. Check the console for details.');
        alert(" Register faild");
      }

  };

  return (
  <div className="full-height">
    <div className='stylescontainer'>
    <div className="bengurionlogo-register">
        <img src="/images/BenGurion.png" alt="Tomato" className="BenGurionimage-register" />
      </div>
      <div className="Tomatologo">
        <img src="/images/logo.png" alt="Tomato" className="Tomatoimage" />
      </div>
        <h2 className='stylestitle'>Tomato Data Project -Register</h2>

      <label className='styleslabel'>Name:</label>
      <input type="text" className='stylesinput' value={name} onChange={(e) => setName(e.target.value)} 
      placeholder="Enter your name"/>
      {errors.name && <p className='styleserror'>{errors.name}</p>}

      <label className='styleslabel'>Email:</label>
      <input type="email" className='stylesinput' value={email} onChange={(e) => setEmail(e.target.value)} 
      placeholder="Enter your email"/>
      {errors.email && <p className='styleserror'>{errors.email}</p>}

      <label className='styleslabel'>Password:</label>
      <input type="password" className='stylesinput' value={password} onChange={(e) => setPassword(e.target.value)}
      placeholder="Enter your password" />
      {errors.password && <p className='styleserror'>{errors.password}</p>}

      <label className='styleslabel'>MopName:</label>
      <input type="text" className='stylesinput' value={mop} onChange={(e) => setMop(e.target.value)}
      placeholder="Enter your MopName" />
      {errors.mop && <p className='styleserror'>{errors.mop}</p>}

      <button className='stylesbutton' onClick={handleRegister}>Register</button>
      <button className='stylesbuttongotologin' onClick={() => navigate('/login')}>
          Go to Login
        </button>
    </div>
    </div>
  );
};

export default Register;
