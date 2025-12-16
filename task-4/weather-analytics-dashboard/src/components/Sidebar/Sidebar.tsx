import React from 'react';
import { MenuItemType } from '../../App';
import './Sidebar.css';

interface SidebarProps {
  activeMenu: MenuItemType;
  onMenuChange: (menu: MenuItemType) => void;
}

const Sidebar: React.FC<SidebarProps> = ({ activeMenu, onMenuChange }) => {
  const menuItems: MenuItemType[] = [
    'Overview',
    'Temperature',
    'Precipitation',
    'Extreme Weather Conditions'
  ];

  const getMenuIcon = (item: MenuItemType): string => {
    switch (item) {
      case 'Overview':
        return '📊';
      case 'Temperature':
        return '🌡️';
      case 'Precipitation':
        return '🌧️';
      case 'Extreme Weather Conditions':
        return '⚠️';
      default:
        return '📌';
    }
  };

  return (
    <div className="sidebar">
      <div className="sidebar-header">
        <h1 className="sidebar-title">Weather Analytics</h1>
      </div>
      <nav className="sidebar-nav">
        {menuItems.map((item) => (
          <div
            key={item}
            className={`menu-item ${activeMenu === item ? 'active' : ''}`}
            onClick={() => onMenuChange(item)}
            role="button"
            tabIndex={0}
            onKeyPress={(e) => {
              if (e.key === 'Enter' || e.key === ' ') {
                onMenuChange(item);
              }
            }}
          >
            <span className="menu-icon">{getMenuIcon(item)}</span>
            <span className="menu-label">{item}</span>
          </div>
        ))}
      </nav>
    </div>
  );
};

export default Sidebar;
