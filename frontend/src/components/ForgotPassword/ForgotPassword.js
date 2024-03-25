import React, { useState } from 'react';
import { Link} from 'react-router-dom';
import './ForgotPassword.css';

const ForgotPassword = () => {
  const [email, setEmail] = useState('');

  const handleForgotPassword = () => {
    // Add your logic for handling forgot password here
    console.log('Forgot password for:', email);
  };

  return (
    <div className='stylescontainer'>
      <h2 className='stylestitle'>Forgot Password</h2>
      <label className='styleslabel'>Email:</label>
      <input type="email" className='stylesinput' value={email} onChange={(e) => setEmail(e.target.value)} />

      <button className='stylesbutton' onClick={handleForgotPassword}>Submit</button>
      <p className='styleslink'>
          <Link to="/login"> log in</Link>.
      </p>
    </div>
  );
};

export default ForgotPassword;
