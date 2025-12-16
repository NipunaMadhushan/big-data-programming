import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import './ExtremeWeather.css';

interface WeatherRecord {
    district: string;
    year: number;
    month: number;
    extreme_weather_days: number;
}

interface YearlyExtremeCount {
    year: number;
    extreme_days: number;
}

const ExtremeWeather: React.FC = () => {
    const [weatherData, setWeatherData] = useState<WeatherRecord[]>([]);
    const [selectedDistrict, setSelectedDistrict] = useState<string>('All');
    const [selectedMonth, setSelectedMonth] = useState<string>('All');
    const [districts, setDistricts] = useState<string[]>([]);
    const [chartData, setChartData] = useState<YearlyExtremeCount[]>([]);
    const [loading, setLoading] = useState<boolean>(true);
    const [thresholds] = useState({ precipitation: 50, windGust: 50 });
    const [totalExtremeDays, setTotalExtremeDays] = useState<number>(0);

    const monthNames = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ];

    // Load weather data from overview-data.csv
    useEffect(() => {
        setLoading(true);
        fetch('/data/overview-data.csv')
            .then(response => response.text())
            .then(text => {
                const lines = text.split('\n').filter(line => line.trim() !== '');
                const data: WeatherRecord[] = [];

                // Parse header to find column indices
                const headers = lines[0].split(',').map(h => h.trim());
                const districtIdx = headers.indexOf('district');
                const yearIdx = headers.indexOf('year');
                const monthIdx = headers.indexOf('month');
                const extremeDaysIdx = headers.indexOf('extreme_weather_days');

                for (let i = 1; i < lines.length; i++) {
                    const values = lines[i].split(',');
                    if (values.length >= headers.length && extremeDaysIdx !== -1) {
                        data.push({
                            district: values[districtIdx].trim(),
                            year: parseInt(values[yearIdx]),
                            month: parseInt(values[monthIdx]),
                            extreme_weather_days: parseInt(values[extremeDaysIdx]) || 0
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

    // Calculate extreme weather days by year
    useEffect(() => {
        if (weatherData.length === 0) return;

        let filteredData = weatherData;

        // Filter by district
        if (selectedDistrict !== 'All') {
            filteredData = filteredData.filter(d => d.district === selectedDistrict);
        }

        // Filter by month
        if (selectedMonth !== 'All') {
            const monthNum = parseInt(selectedMonth);
            filteredData = filteredData.filter(d => d.month === monthNum);
        }

        // Sum extreme days by year
        const yearCounts: { [year: number]: number } = {};
        let totalDays = 0;

        filteredData.forEach(record => {
            yearCounts[record.year] = (yearCounts[record.year] || 0) + record.extreme_weather_days;
            totalDays += record.extreme_weather_days;
        });

        // Get all unique years from data
        const allYears = Array.from(new Set(filteredData.map(d => d.year))).sort();

        const yearlyData = allYears.map(year => ({
            year,
            extreme_days: yearCounts[year] || 0
        }));

        setChartData(yearlyData);
        setTotalExtremeDays(totalDays);
    }, [weatherData, selectedDistrict, selectedMonth]);

    // Custom tooltip for the chart
    const CustomTooltip = ({ active, payload }: any) => {
        if (active && payload && payload.length) {
            return (
                <div className="custom-tooltip">
                    <p className="tooltip-year">{`Year: ${payload[0].payload.year}`}</p>
                    <p className="tooltip-value">{`${payload[0].value} days`}</p>
                    <p className="tooltip-label">with extreme weather</p>
                </div>
            );
        }
        return null;
    };

    return (
        <div className="component-container">
            <div className="component-header">
                <h2>Extreme Weather Events</h2>
                <p className="subtitle">Analysis of extreme weather days with high precipitation ({thresholds.precipitation}mm) and high wind gusts ({thresholds.windGust}km/h)</p>
            </div>

            <div className="component-content">
                {loading ? (
                    <div className="loading-message">
                        <p>Loading extreme weather data...</p>
                        <p className="loading-subtext">Processing monthly aggregated data...</p>
                    </div>
                ) : weatherData.length === 0 ? (
                    <div className="no-data-message">
                        <p>No extreme weather data available.</p>
                        <p className="no-data-subtext">Please ensure the CSV file includes the 'extreme_weather_days' column.</p>
                    </div>
                ) : (
                    <>
                        {/* Filters Section */}
                        <div className="filters-section">
                            <div className="filter-group">
                                <label htmlFor="district-select-extreme">District:</label>
                                <select
                                    id="district-select-extreme"
                                    value={selectedDistrict}
                                    onChange={(e) => setSelectedDistrict(e.target.value)}
                                    className="filter-select"
                                >
                                    <option value="All">All Districts</option>
                                    {districts.map(district => (
                                        <option key={district} value={district}>{district}</option>
                                    ))}
                                </select>
                            </div>

                            <div className="filter-group">
                                <label htmlFor="month-select-extreme">Month:</label>
                                <select
                                    id="month-select-extreme"
                                    value={selectedMonth}
                                    onChange={(e) => setSelectedMonth(e.target.value)}
                                    className="filter-select"
                                >
                                    <option value="All">All Months</option>
                                    {monthNames.map((month, index) => (
                                        <option key={index + 1} value={index + 1}>{month}</option>
                                    ))}
                                </select>
                            </div>
                        </div>

                        {/* Summary Cards */}
                        <div className="summary-cards">
                            <div className="summary-card total">
                                <div className="card-icon">⚠️</div>
                                <div className="card-content">
                                    <div className="card-value">{totalExtremeDays.toLocaleString()}</div>
                                    <div className="card-label">Total Extreme Weather Days</div>
                                </div>
                            </div>

                            <div className="summary-card threshold">
                                <div className="card-icon">💧</div>
                                <div className="card-content">
                                    <div className="card-value">&gt;{thresholds.precipitation}mm</div>
                                    <div className="card-label">Precipitation Threshold</div>
                                </div>
                            </div>

                            <div className="summary-card threshold">
                                <div className="card-icon">💨</div>
                                <div className="card-content">
                                    <div className="card-value">&gt;{thresholds.windGust}km/h</div>
                                    <div className="card-label">Wind Gust Threshold</div>
                                </div>
                            </div>

                            <div className="summary-card average">
                                <div className="card-icon">📊</div>
                                <div className="card-content">
                                    <div className="card-value">
                                        {chartData.length > 0
                                            ? (totalExtremeDays / chartData.length).toFixed(1)
                                            : '0'}
                                    </div>
                                    <div className="card-label">Avg. Days per Year</div>
                                </div>
                            </div>
                        </div>

                        {/* Line Chart */}
                        {chartData.length > 0 ? (
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
                                            label={{ value: 'Number of Extreme Weather Days', angle: -90, position: 'insideLeft', style: { fontSize: '14px', fontWeight: 600 } }}
                                            tick={{ fontSize: 12 }}
                                            stroke="#6b7280"
                                            allowDecimals={false}
                                        />
                                        <Tooltip content={<CustomTooltip />} />
                                        <Legend
                                            wrapperStyle={{ paddingTop: '20px' }}
                                            iconType="line"
                                        />
                                        <Line
                                            type="monotone"
                                            dataKey="extreme_days"
                                            stroke="#DC2626"
                                            strokeWidth={3}
                                            dot={{ fill: '#DC2626', r: 5 }}
                                            activeDot={{ r: 7 }}
                                            name="Extreme Weather Days"
                                        />
                                    </LineChart>
                                </ResponsiveContainer>

                                {/* Insights Section */}
                                <div className="chart-insights">
                                    <div className="insight-card">
                                        <h4>📈 Analysis Insights</h4>
                                        <div className="insight-content">
                                            {selectedDistrict === 'All' && selectedMonth === 'All' && (
                                                <p>This chart shows the total number of extreme weather days across all districts and months from 2010 to 2024.</p>
                                            )}
                                            {selectedDistrict !== 'All' && selectedMonth === 'All' && (
                                                <p>This chart shows extreme weather days in <strong>{selectedDistrict}</strong> across all months from 2010 to 2024.</p>
                                            )}
                                            {selectedDistrict === 'All' && selectedMonth !== 'All' && (
                                                <p>This chart shows extreme weather days in <strong>{monthNames[parseInt(selectedMonth) - 1]}</strong> across all districts from 2010 to 2024.</p>
                                            )}
                                            {selectedDistrict !== 'All' && selectedMonth !== 'All' && (
                                                <p>This chart shows extreme weather days in <strong>{selectedDistrict}</strong> during <strong>{monthNames[parseInt(selectedMonth) - 1]}</strong> from 2010 to 2024.</p>
                                            )}

                                            {chartData.length > 0 && (
                                                <div className="insight-stats">
                                                    <div className="stat-item-inline">
                                                        <span className="stat-label-inline">Most Extreme Year:</span>
                                                        <span className="stat-value-inline">
                              {chartData.reduce((max, d) => d.extreme_days > max.extreme_days ? d : max).year}
                                                            {' '}({chartData.reduce((max, d) => d.extreme_days > max.extreme_days ? d : max).extreme_days} days)
                            </span>
                                                    </div>
                                                    <div className="stat-item-inline">
                                                        <span className="stat-label-inline">Least Extreme Year:</span>
                                                        <span className="stat-value-inline">
                              {chartData.reduce((min, d) => d.extreme_days < min.extreme_days ? d : min).year}
                                                            {' '}({chartData.reduce((min, d) => d.extreme_days < min.extreme_days ? d : min).extreme_days} days)
                            </span>
                                                    </div>
                                                    <div className="stat-item-inline">
                                                        <span className="stat-label-inline">Total Period:</span>
                                                        <span className="stat-value-inline">
                              {chartData.length} years analyzed
                            </span>
                                                    </div>
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        ) : (
                            <div className="no-data-message">
                                <p>No extreme weather events found for the selected filters.</p>
                                <p className="no-data-subtext">Try adjusting the district or month selection.</p>
                            </div>
                        )}

                        {/* Threshold Information */}
                        <div className="threshold-info">
                            <h4>🎯 Extreme Weather Definition</h4>
                            <p>An extreme weather day is defined as a day when <strong>both</strong> of the following conditions are met:</p>
                            <ul>
                                <li><strong>High Precipitation:</strong> Daily precipitation sum exceeds {thresholds.precipitation}mm</li>
                                <li><strong>High Wind Gusts:</strong> Maximum wind gust speed exceeds {thresholds.windGust}km/h</li>
                            </ul>
                            <p className="threshold-note">
                                These thresholds represent the combination of heavy rainfall with strong winds, which typically indicates storms, cyclones, or severe weather systems that can cause significant disruption and damage.
                            </p>
                        </div>
                    </>
                )}
            </div>
        </div>
    );
};

export default ExtremeWeather;
