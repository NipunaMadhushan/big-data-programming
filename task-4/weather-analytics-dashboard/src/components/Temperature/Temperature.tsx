import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import './Temperature.css';

interface WeatherData {
    district: string;
    year: number;
    month: number;
    mean_temperature_c: number;
}

interface YearlyPercentage {
    year: number;
    percentage: number;
}

interface DistrictYearlyData {
    [district: string]: YearlyPercentage[];
}

const Temperature: React.FC = () => {
    const [weatherData, setWeatherData] = useState<WeatherData[]>([]);
    const [selectedDistrict, setSelectedDistrict] = useState<string>('All');
    const [districts, setDistricts] = useState<string[]>([]);
    const [chartData, setChartData] = useState<any[]>([]);
    const [loading, setLoading] = useState<boolean>(true);

    // Load data from CSV
    useEffect(() => {
        setLoading(true);
        fetch('/data/overview-data.csv')
            .then(response => response.text())
            .then(text => {
                const lines = text.split('\n').filter(line => line.trim() !== '');
                const data: WeatherData[] = [];

                for (let i = 1; i < lines.length; i++) {
                    const values = lines[i].split(',');
                    if (values.length >= 6) {
                        data.push({
                            district: values[0].trim(),
                            year: parseInt(values[1]),
                            month: parseInt(values[2]),
                            mean_temperature_c: parseFloat(values[5])
                        });
                    }
                }

                setWeatherData(data);

                // Extract unique districts
                const uniqueDistricts = Array.from(new Set(data.map(d => d.district))).sort();
                setDistricts(uniqueDistricts);
                setLoading(false);
            })
            .catch(error => {
                console.error('Error loading weather data:', error);
                setLoading(false);
            });
    }, []);

    // Calculate percentage for each district and year
    useEffect(() => {
        if (weatherData.length === 0) return;

        const districtYearlyData: DistrictYearlyData = {};

        // Get unique years
        const years = Array.from(new Set(weatherData.map(d => d.year))).sort();

        if (selectedDistrict === 'All') {
            // Calculate average percentage across all districts for each year
            const yearlyData: { [year: number]: { above30: number; total: number } } = {};

            weatherData.forEach(record => {
                if (!yearlyData[record.year]) {
                    yearlyData[record.year] = { above30: 0, total: 0 };
                }
                yearlyData[record.year].total += 1;
                if (record.mean_temperature_c > 30) {
                    yearlyData[record.year].above30 += 1;
                }
            });

            const data = years.map(year => ({
                year,
                percentage: yearlyData[year]
                    ? (yearlyData[year].above30 / yearlyData[year].total) * 100
                    : 0
            }));

            setChartData(data);
        } else {
            // Calculate for specific district
            const districtData = weatherData.filter(d => d.district === selectedDistrict);
            const yearlyData: { [year: number]: { above30: number; total: number } } = {};

            districtData.forEach(record => {
                if (!yearlyData[record.year]) {
                    yearlyData[record.year] = { above30: 0, total: 0 };
                }
                yearlyData[record.year].total += 1;
                if (record.mean_temperature_c > 30) {
                    yearlyData[record.year].above30 += 1;
                }
            });

            const data = years.map(year => ({
                year,
                percentage: yearlyData[year]
                    ? (yearlyData[year].above30 / yearlyData[year].total) * 100
                    : 0
            }));

            setChartData(data);
        }
    }, [weatherData, selectedDistrict]);

    // Custom tooltip for the chart
    const CustomTooltip = ({ active, payload }: any) => {
        if (active && payload && payload.length) {
            return (
                <div className="custom-tooltip">
                    <p className="tooltip-year">{`Year: ${payload[0].payload.year}`}</p>
                    <p className="tooltip-value">{`${payload[0].value.toFixed(1)}%`}</p>
                    <p className="tooltip-label">of months above 30°C</p>
                </div>
            );
        }
        return null;
    };

    return (
        <div className="component-container">
            <div className="component-header">
                <h2>Temperature Analysis</h2>
                <p className="subtitle">Percentage of months with mean temperature above 30°C</p>
            </div>

            <div className="component-content">
                <div className="filters-section">
                    <div className="filter-group">
                        <label htmlFor="district-select-temp">District:</label>
                        <select
                            id="district-select-temp"
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
                </div>

                {loading ? (
                    <div className="loading-message">
                        <p>Loading data...</p>
                    </div>
                ) : chartData.length > 0 ? (
                    <div className="chart-container">
                        <ResponsiveContainer width="100%" height={450}>
                            <LineChart
                                data={chartData}
                                margin={{ top: 20, right: 30, left: 20, bottom: 60 }}
                            >
                                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                                <XAxis
                                    dataKey="year"
                                    label={{ value: 'Year', position: 'insideBottom', offset: -10, style: { fontSize: '14px', fontWeight: 600 } }}
                                    tick={{ fontSize: 12 }}
                                    stroke="#6b7280"
                                />
                                <YAxis
                                    label={{ value: 'Percentage (%)', angle: -90, position: 'insideLeft', style: { fontSize: '14px', fontWeight: 600 } }}
                                    tick={{ fontSize: 12 }}
                                    stroke="#6b7280"
                                    domain={[0, 100]}
                                />
                                <Tooltip content={<CustomTooltip />} />
                                <Legend
                                    wrapperStyle={{ paddingTop: '20px' }}
                                    iconType="line"
                                />
                                <Line
                                    type="monotone"
                                    dataKey="percentage"
                                    stroke="#2B62C4"
                                    strokeWidth={3}
                                    dot={{ fill: '#2B62C4', r: 5 }}
                                    activeDot={{ r: 7 }}
                                    name="Percentage of Months Above 30°C"
                                />
                            </LineChart>
                        </ResponsiveContainer>

                        <div className="chart-insights">
                            <div className="insight-card">
                                <h4>Analysis Insights</h4>
                                <div className="insight-content">
                                    {selectedDistrict === 'All' ? (
                                        <p>This chart shows the average percentage of months with mean temperature above 30°C across all districts from 2010 to 2024.</p>
                                    ) : (
                                        <p>This chart shows the percentage of months with mean temperature above 30°C in <strong>{selectedDistrict}</strong> from 2010 to 2024.</p>
                                    )}
                                    <div className="insight-stats">
                                        <div className="stat-item-inline">
                                            <span className="stat-label-inline">Highest:</span>
                                            <span className="stat-value-inline">
                        {Math.max(...chartData.map(d => d.percentage)).toFixed(1)}%
                                                {' '}in {chartData.find(d => d.percentage === Math.max(...chartData.map(d => d.percentage)))?.year}
                      </span>
                                        </div>
                                        <div className="stat-item-inline">
                                            <span className="stat-label-inline">Lowest:</span>
                                            <span className="stat-value-inline">
                        {Math.min(...chartData.map(d => d.percentage)).toFixed(1)}%
                                                {' '}in {chartData.find(d => d.percentage === Math.min(...chartData.map(d => d.percentage)))?.year}
                      </span>
                                        </div>
                                        <div className="stat-item-inline">
                                            <span className="stat-label-inline">Average:</span>
                                            <span className="stat-value-inline">
                        {(chartData.reduce((sum, d) => sum + d.percentage, 0) / chartData.length).toFixed(1)}%
                      </span>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                ) : (
                    <div className="no-data-message">
                        <p>No data available for the selected district.</p>
                    </div>
                )}
            </div>
        </div>
    );
};

export default Temperature;
