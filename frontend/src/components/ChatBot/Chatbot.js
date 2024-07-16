import React, { useState } from 'react';
import '../../pages/ExportData.css'; // Adjust this path if necessary

const Chatbot = () => {
    const [messages, setMessages] = useState([]);
    const [input, setInput] = useState('');

    const sendMessage = async () => {
        if (input.trim() === '') return;

        const userMessage = { sender: 'user', text: input };
        setMessages([...messages, userMessage]);
        setInput('');

        // Show a message indicating data is being sent for processing
        const processingMessage = { sender: 'bot', text: 'Sending data to Assistant for processing...' };
        setMessages(prevMessages => [...prevMessages, processingMessage]);

        try {
            const response = await fetch('http://localhost:5000/mock', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ question: input })
            });
            const data = await response.json();
            const filePath = data.response;
            const botMessage = { sender: 'bot', text: `Here is your report: <a href="http://localhost:5000/view_report/${filePath}" target="_blank">View Report</a>` };
            setMessages(prevMessages => [...prevMessages, botMessage]);
        } catch (error) {
            console.error('Error communicating with the bot:', error);
            const errorMessage = { sender: 'bot', text: 'Error communicating with the bot. Please try again later.' };
            setMessages(prevMessages => [...prevMessages, errorMessage]);
        }
    };

    return (
        <div className="chatbot-container">
            <div className="chatbot-messages">
                {messages.map((msg, index) => (
                    <div key={index} className={`chatbot-message ${msg.sender}`}>
                        {msg.sender === 'bot' ? (
                            <div dangerouslySetInnerHTML={{ __html: msg.text }} />
                        ) : (
                            msg.text
                        )}
                    </div>
                ))}
            </div>
            <div className="chatbot-input">
                <input
                    type="text"
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && sendMessage()}
                    placeholder="Type your message..."
                />
                <button onClick={sendMessage}>Send</button>
            </div>
        </div>
    );
};

export default Chatbot;
