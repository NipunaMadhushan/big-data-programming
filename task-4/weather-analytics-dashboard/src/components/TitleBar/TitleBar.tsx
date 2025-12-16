import React from 'react';
import './TitleBar.css';

interface TitleBarProps {
    title: string;
}

const TitleBar: React.FC<TitleBarProps> = ({ title }) => {
    return (
        <div className="title-bar">
            <h1 className="page-title">Sri Lanka Weather Analytics Dashboard</h1>
        </div>
    );
};

export default TitleBar;
