import React from 'react';

const DashEmbed = () => {
    return (
        <div style={{ height: '100vh', width: '100%' }}>
            <iframe
                src="http://127.0.0.1:8050/"
                style={{ border: 'none', height: '100%', width: '100%' }}
                title="Dash Application"
            />
        </div>
    );
};

export default DashEmbed;
