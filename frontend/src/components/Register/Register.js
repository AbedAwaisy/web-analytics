import React, { useState } from 'react';
import { Link} from 'react-router-dom';
import './Register.css';
import { insertUserToDB } from '../../api/api'; // Adjust the path based on the actual location of api.js

const Register = () => {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [mop, setMop] = useState('');
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
    <div className='stylescontainer'>
      <h2 className='stylestitle'>Register</h2>

      <label className='styleslabel'>Name:</label>
      <input type="text" className='stylesinput' value={name} onChange={(e) => setName(e.target.value)} />
      {errors.name && <p className='styleserror'>{errors.name}</p>}

      <label className='styleslabel'>Email:</label>
      <input type="email" className='stylesinput' value={email} onChange={(e) => setEmail(e.target.value)} />
      {errors.email && <p className='styleserror'>{errors.email}</p>}

      <label className='styleslabel'>Password:</label>
      <input type="password" className='stylesinput' value={password} onChange={(e) => setPassword(e.target.value)} />
      {errors.password && <p className='styleserror'>{errors.password}</p>}

      <label className='styleslabel'>MopName:</label>
      <input type="text" className='stylesinput' value={mop} onChange={(e) => setMop(e.target.value)} />
      {errors.mop && <p className='styleserror'>{errors.mop}</p>}

      <button className='stylesbutton' onClick={handleRegister}>Register</button>
      <p className='styleslink'>
          <Link to="/login"> log in</Link>.
      </p>
    </div>
  );
};

export default Register;
