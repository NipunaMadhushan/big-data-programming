import React, { useState, useEffect } from 'react';
import './Overview.css';

interface WeatherData {
    district: string;
    year: number;
    month: number;
    min_temperature_c: number;
    max_temperature_c: number;
    mean_temperature_c: number;
    avg_wind_speed_kmh: number;
    avg_shortwave_radiation_mj_m2: number;
    avg_evapotranspiration_mm: number;
}

interface AggregatedData {
    min_temperature_c: number;
    max_temperature_c: number;
    mean_temperature_c: number;
    avg_wind_speed_kmh: number;
    avg_shortwave_radiation_mj_m2: number;
    avg_evapotranspiration_mm: number;
}

const Overview: React.FC = () => {
    const [weatherData, setWeatherData] = useState<WeatherData[]>([]);
    const [selectedDistrict, setSelectedDistrict] = useState<string>('All');
    const [selectedYear, setSelectedYear] = useState<string>('All');
    const [selectedMonth, setSelectedMonth] = useState<string>('All');
    const [aggregatedData, setAggregatedData] = useState<AggregatedData | null>(null);
    const [districts, setDistricts] = useState<string[]>([]);
    const [years, setYears] = useState<number[]>([]);

    const monthNames = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ];

    useEffect(() => {
        // Load CSV data
        fetch('/data/overview-data.csv')
            .then(response => response.text())
            .then(text => {
                const lines = text.split('\n').filter(line => line.trim() !== '');
                const headers = lines[0].split(',');

                const data: WeatherData[] = [];
                for (let i = 1; i < lines.length; i++) {
                    const values = lines[i].split(',');
                    if (values.length === headers.length) {
                        data.push({
                            district: values[0].trim(),
                            year: parseInt(values[1]),
                            month: parseInt(values[2]),
                            min_temperature_c: parseFloat(values[3]),
                            max_temperature_c: parseFloat(values[4]),
                            mean_temperature_c: parseFloat(values[5]),
                            avg_wind_speed_kmh: parseFloat(values[6]),
                            avg_shortwave_radiation_mj_m2: parseFloat(values[10]),
                            avg_evapotranspiration_mm: parseFloat(values[11])
                        });
                    }
                }

                setWeatherData(data);

                // Extract unique districts and years
                const uniqueDistricts = Array.from(new Set(data.map(d => d.district))).sort();
                const uniqueYears = Array.from(new Set(data.map(d => d.year))).sort();

                setDistricts(uniqueDistricts);
                setYears(uniqueYears);
            })
            .catch(error => console.error('Error loading weather data:', error));
    }, []);

    useEffect(() => {
        // Filter and aggregate data based on selections
        let filteredData = weatherData;

        if (selectedDistrict !== 'All') {
            filteredData = filteredData.filter(d => d.district === selectedDistrict);
        }

        if (selectedYear !== 'All') {
            filteredData = filteredData.filter(d => d.year === parseInt(selectedYear));
        }

        if (selectedMonth !== 'All') {
            filteredData = filteredData.filter(d => d.month === parseInt(selectedMonth));
        }

        if (filteredData.length > 0) {
            // Calculate averages
            const aggregated: AggregatedData = {
                min_temperature_c: filteredData.reduce((sum, d) => sum + d.min_temperature_c, 0) / filteredData.length,
                max_temperature_c: filteredData.reduce((sum, d) => sum + d.max_temperature_c, 0) / filteredData.length,
                mean_temperature_c: filteredData.reduce((sum, d) => sum + d.mean_temperature_c, 0) / filteredData.length,
                avg_wind_speed_kmh: filteredData.reduce((sum, d) => sum + d.avg_wind_speed_kmh, 0) / filteredData.length,
                avg_shortwave_radiation_mj_m2: filteredData.reduce((sum, d) => sum + d.avg_shortwave_radiation_mj_m2, 0) / filteredData.length,
                avg_evapotranspiration_mm: filteredData.reduce((sum, d) => sum + d.avg_evapotranspiration_mm, 0) / filteredData.length
            };

            setAggregatedData(aggregated);
        } else {
            setAggregatedData(null);
        }
    }, [selectedDistrict, selectedYear, selectedMonth, weatherData]);

    return (
        <div className="component-container">
            <div className="component-header">
                <h2>Overview</h2>
                <p className="subtitle">Weather analysis over the past years (2010-2024)</p>
            </div>

            <div className="component-content">
                <div className="filters-section">
                    <div className="filter-group">
                        <label htmlFor="district-select">District:</label>
                        <select
                            id="district-select"
                            value={selectedDistrict}
                            onChange={(e) => setSelectedDistrict(e.target.value)}
                            className="filter-select"
                        >
                            <option value="All">All</option>
                            {districts.map(district => (
                                <option key={district} value={district}>{district}</option>
                            ))}
                        </select>
                    </div>

                    <div className="filter-group">
                        <label htmlFor="year-select">Year:</label>
                        <select
                            id="year-select"
                            value={selectedYear}
                            onChange={(e) => setSelectedYear(e.target.value)}
                            className="filter-select"
                        >
                            <option value="All">All</option>
                            {years.map(year => (
                                <option key={year} value={year}>{year}</option>
                            ))}
                        </select>
                    </div>

                    <div className="filter-group">
                        <label htmlFor="month-select">Month:</label>
                        <select
                            id="month-select"
                            value={selectedMonth}
                            onChange={(e) => setSelectedMonth(e.target.value)}
                            className="filter-select"
                        >
                            <option value="All">All</option>
                            {monthNames.map((month, index) => (
                                <option key={index + 1} value={index + 1}>{month}</option>
                            ))}
                        </select>
                    </div>
                </div>

                {aggregatedData ? (
                    <div className="statistics-grid">
                        <div className="stat-card">
                            <div className="stat-icon temperature-icon">🌡️</div>
                            <div className="stat-content">
                                <h3>Temperature</h3>
                                <div className="stat-values">
                                    <div className="stat-item">
                                        <span className="stat-label">Min:</span>
                                        <span className="stat-value">{aggregatedData.min_temperature_c.toFixed(1)}°C</span>
                                    </div>
                                    <div className="stat-item">
                                        <span className="stat-label">Max:</span>
                                        <span className="stat-value">{aggregatedData.max_temperature_c.toFixed(1)}°C</span>
                                    </div>
                                    <div className="stat-item">
                                        <span className="stat-label">Mean:</span>
                                        <span className="stat-value">{aggregatedData.mean_temperature_c.toFixed(1)}°C</span>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <div className="stat-card">
                            <div className="stat-icon wind-icon">💨</div>
                            <div className="stat-content">
                                <h3>Wind Speed</h3>
                                <div className="stat-single">
                                    <span className="stat-value-large">{aggregatedData.avg_wind_speed_kmh.toFixed(1)}</span>
                                    <span className="stat-unit">km/h</span>
                                </div>
                            </div>
                        </div>

                        <div className="stat-card">
                            <div className="stat-icon radiation-icon">☀️</div>
                            <div className="stat-content">
                                <h3>Shortwave Radiation</h3>
                                <div className="stat-single">
                                    <span className="stat-value-large">{aggregatedData.avg_shortwave_radiation_mj_m2.toFixed(1)}</span>
                                    <span className="stat-unit">MJ/m²</span>
                                </div>
                            </div>
                        </div>

                        <div className="stat-card">
                            <div className="stat-icon evapotranspiration-icon">💧</div>
                            <div className="stat-content">
                                <h3>Evapotranspiration</h3>
                                <div className="stat-single">
                                    <span className="stat-value-large">{aggregatedData.avg_evapotranspiration_mm.toFixed(1)}</span>
                                    <span className="stat-unit">mm</span>
                                </div>
                            </div>
                        </div>
                    </div>
                ) : (
                    <div className="no-data-message">
                        <p>No data available for the selected filters.</p>
                    </div>
                )}
            </div>
        </div>
    );
};

export default Overview;
