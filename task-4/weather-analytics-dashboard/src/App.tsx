import React, { useState } from 'react';
import './App.css';
import Sidebar from './components/Sidebar/Sidebar';
import TitleBar from './components/TitleBar/TitleBar';
import Overview from './components/Overview/Overview';
import Temperature from './components/Temperature/Temperature';
import Precipitation from './components/Precipitation/Precipitation';
import ExtremeWeather from './components/ExtremeWeather/ExtremeWeather';

export type MenuItemType = 'Overview' | 'Temperature' | 'Precipitation' | 'Extreme Weather Conditions';

const App: React.FC = () => {
  const [activeMenu, setActiveMenu] = useState<MenuItemType>('Overview');

  const renderContent = () => {
    switch (activeMenu) {
      case 'Overview':
        return <Overview />;
      case 'Temperature':
        return <Temperature />;
      case 'Precipitation':
        return <Precipitation />;
      case 'Extreme Weather Conditions':
        return <ExtremeWeather />;
      default:
        return <Overview />;
    }
  };

  return (
    <div className="app">
      <Sidebar activeMenu={activeMenu} onMenuChange={setActiveMenu} />
      <div className="main-content">
        <TitleBar title={activeMenu} />
        <div className="content-area">
          {renderContent()}
        </div>
      </div>
    </div>
  );
};

export default App;
