import React, { useState, useEffect } from 'react';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from 'recharts';
import './Precipitation.css';

interface WeatherData {
    district: string;
    year: number;
    month: number;
    precipitation_sum_mm: number;
    precipitation_hours: number;
}

interface YearlyData {
    year: number;
    precipitation_hours: number;
    precipitation_sum_mm: number;
}

interface DistrictTotal {
    district: string;
    total_precipitation_mm: number;
}

interface MonthlyPrecipitation {
    month: number;
    monthName: string;
    total_precipitation_mm: number;
}

const Precipitation: React.FC = () => {
    const [weatherData, setWeatherData] = useState<WeatherData[]>([]);
    const [selectedDistrict, setSelectedDistrict] = useState<string>('All');
    const [selectedMonth, setSelectedMonth] = useState<string>('All');
    const [districts, setDistricts] = useState<string[]>([]);
    const [yearlyHoursData, setYearlyHoursData] = useState<YearlyData[]>([]);
    const [yearlySumData, setYearlySumData] = useState<YearlyData[]>([]);
    const [top5Districts, setTop5Districts] = useState<DistrictTotal[]>([]);
    const [mostPrecipitousMonths, setMostPrecipitousMonths] = useState<{ [district: string]: MonthlyPrecipitation }>({});
    const [loading, setLoading] = useState<boolean>(true);

    const monthNames = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ];

    const colors = ['#2B62C4', '#3B82F6', '#60A5FA', '#93C5FD', '#BFDBFE'];

    // Load data from CSV
    useEffect(() => {
        setLoading(true);
        fetch('/data/overview-data.csv')
            .then(response => response.text())
            .then(text => {
                const lines = text.split('\n').filter(line => line.trim() !== '');
                const headers = lines[0].split(',').map(h => h.trim());

                // Find column indices
                const districtIdx = headers.indexOf('district');
                const yearIdx = headers.indexOf('year');
                const monthIdx = headers.indexOf('month');
                const precipSumIdx = headers.indexOf('precipitation_sum_mm');
                const precipHoursIdx = headers.indexOf('precipitation_hours_h');

                const data: WeatherData[] = [];

                for (let i = 1; i < lines.length; i++) {
                    const values = lines[i].split(',');
                    if (values.length >= headers.length && precipSumIdx !== -1 && precipHoursIdx !== -1) {
                        data.push({
                            district: values[districtIdx].trim(),
                            year: parseInt(values[yearIdx]),
                            month: parseInt(values[monthIdx]),
                            precipitation_sum_mm: parseFloat(values[precipSumIdx]) || 0,
                            precipitation_hours: parseFloat(values[precipHoursIdx]) || 0
                        });
                    }
                }

                setWeatherData(data);

                // Extract unique districts
                const uniqueDistricts = Array.from(new Set(data.map(d => d.district))).sort();
                setDistricts(uniqueDistricts);

                // Calculate top 5 districts
                calculateTop5Districts(data);

                // Calculate most precipitous months
                calculateMostPrecipitousMonths(data);

                setLoading(false);
            })
            .catch(error => {
                console.error('Error loading weather data:', error);
                setLoading(false);
            });
    }, []);

    // Calculate top 5 districts by total precipitation
    const calculateTop5Districts = (data: WeatherData[]) => {
        const districtTotals: { [district: string]: number } = {};

        data.forEach(record => {
            if (!districtTotals[record.district]) {
                districtTotals[record.district] = 0;
            }
            districtTotals[record.district] += record.precipitation_sum_mm;
        });

        const sorted = Object.entries(districtTotals)
            .map(([district, total]) => ({ district, total_precipitation_mm: total }))
            .sort((a, b) => b.total_precipitation_mm - a.total_precipitation_mm)
            .slice(0, 5);

        setTop5Districts(sorted);
    };

    // Calculate most precipitous month for each district
    const calculateMostPrecipitousMonths = (data: WeatherData[]) => {
        const districtMonthly: { [district: string]: { [month: number]: number } } = {};

        data.forEach(record => {
            if (!districtMonthly[record.district]) {
                districtMonthly[record.district] = {};
            }
            if (!districtMonthly[record.district][record.month]) {
                districtMonthly[record.district][record.month] = 0;
            }
            districtMonthly[record.district][record.month] += record.precipitation_sum_mm;
        });

        const mostPrecipitous: { [district: string]: MonthlyPrecipitation } = {};

        Object.entries(districtMonthly).forEach(([district, months]) => {
            const monthData = Object.entries(months).map(([month, total]) => ({
                month: parseInt(month),
                monthName: monthNames[parseInt(month) - 1],
                total_precipitation_mm: total
            }));

            const max = monthData.reduce((prev, current) =>
                (prev.total_precipitation_mm > current.total_precipitation_mm) ? prev : current
            );

            mostPrecipitous[district] = max;
        });

        setMostPrecipitousMonths(mostPrecipitous);
    };

    // Calculate yearly data for line charts
    useEffect(() => {
        if (weatherData.length === 0) return;

        const years = Array.from(new Set(weatherData.map(d => d.year))).sort();

        // Apply filters
        let filteredData = weatherData;

        if (selectedDistrict !== 'All') {
            filteredData = filteredData.filter(d => d.district === selectedDistrict);
        }

        if (selectedMonth !== 'All') {
            const monthNum = parseInt(selectedMonth);
            filteredData = filteredData.filter(d => d.month === monthNum);
        }

        // Calculate average across filtered data
        const yearlyData: { [year: number]: { hours: number; sum: number; count: number } } = {};

        filteredData.forEach(record => {
            if (!yearlyData[record.year]) {
                yearlyData[record.year] = { hours: 0, sum: 0, count: 0 };
            }
            yearlyData[record.year].hours += record.precipitation_hours;
            yearlyData[record.year].sum += record.precipitation_sum_mm;
            yearlyData[record.year].count += 1;
        });

        const hoursData = years.map(year => ({
            year,
            precipitation_hours: yearlyData[year] ? yearlyData[year].hours / yearlyData[year].count : 0,
            precipitation_sum_mm: 0
        }));

        const sumData = years.map(year => ({
            year,
            precipitation_hours: 0,
            precipitation_sum_mm: yearlyData[year] ? yearlyData[year].sum / yearlyData[year].count : 0
        }));

        setYearlyHoursData(hoursData);
        setYearlySumData(sumData);
    }, [weatherData, selectedDistrict, selectedMonth]);

    // Custom tooltip for line charts
    const CustomLineTooltip = ({ active, payload, label, unit }: any) => {
        if (active && payload && payload.length) {
            return (
                <div className="custom-tooltip">
                    <p className="tooltip-year">{`Year: ${label}`}</p>
                    <p className="tooltip-value">{`${payload[0].value.toFixed(1)} ${unit}`}</p>
                </div>
            );
        }
        return null;
    };

    return (
        <div className="component-container">
            <div className="component-header">
                <h2>Precipitation Analysis</h2>
                <p className="subtitle">Comprehensive precipitation patterns and trends analysis (2010-2024)</p>
            </div>

            <div className="component-content">
                {loading ? (
                    <div className="loading-message">
                        <p>Loading precipitation data...</p>
                    </div>
                ) : weatherData.length === 0 ? (
                    <div className="no-data-message">
                        <p>No precipitation data available. Please ensure the CSV file includes 'precipitation_sum_mm' and 'precipitation_hours' columns.</p>
                    </div>
                ) : (
                    <>
                        {/* Analytics Insights Section */}
                        <div className="analytics-section">
                            <h3 className="section-title">📊 Analytics Insights</h3>

                            {/* Top 5 Districts */}
                            <div className="insight-container">
                                <h4 className="insight-title">Top 5 Districts by Total Precipitation</h4>
                                <div className="chart-wrapper">
                                    <ResponsiveContainer width="100%" height={300}>
                                        <BarChart
                                            data={top5Districts}
                                            margin={{ top: 20, right: 30, left: 20, bottom: 60 }}
                                        >
                                            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                                            <XAxis
                                                dataKey="district"
                                                angle={-45}
                                                textAnchor="end"
                                                height={80}
                                                tick={{ fontSize: 12 }}
                                                stroke="#6b7280"
                                            />
                                            <YAxis
                                                label={{ value: 'Total Precipitation (mm)', angle: -90, position: 'insideLeft', style: { fontSize: '14px', fontWeight: 600 } }}
                                                tick={{ fontSize: 12 }}
                                                stroke="#6b7280"
                                            />
                                            <Tooltip
                                                formatter={(value: any) => [`${value.toFixed(1)} mm`, 'Total Precipitation']}
                                                contentStyle={{ backgroundColor: 'white', border: '1px solid #e5e7eb', borderRadius: '8px' }}
                                            />
                                            <Bar dataKey="total_precipitation_mm" radius={[8, 8, 0, 0]}>
                                                {top5Districts.map((entry, index) => (
                                                    <Cell key={`cell-${index}`} fill={colors[index]} />
                                                ))}
                                            </Bar>
                                        </BarChart>
                                    </ResponsiveContainer>
                                </div>
                            </div>

                            {/* Most Precipitous Months */}
                            <div className="insight-container">
                                <h4 className="insight-title">Most Precipitous Month/Season by District</h4>
                                <div className="precipitous-months-grid">
                                    {Object.entries(mostPrecipitousMonths).slice(0, 12).map(([district, data]) => (
                                        <div key={district} className="month-card">
                                            <div className="district-name">{district}</div>
                                            <div className="month-name">{data.monthName}</div>
                                            <div className="month-value">{data.total_precipitation_mm.toFixed(0)} mm</div>
                                        </div>
                                    ))}
                                </div>
                                {Object.keys(mostPrecipitousMonths).length > 12 && (
                                    <div className="view-all-note">
                                        Showing 12 of {Object.keys(mostPrecipitousMonths).length} districts
                                    </div>
                                )}
                            </div>
                        </div>

                        {/* Line Charts Section */}
                        <div className="charts-section">
                            <h3 className="section-title">📈 Precipitation Trends Over Years</h3>

                            <div className="filters-section">
                                <div className="filter-group">
                                    <label htmlFor="district-select-precip">District:</label>
                                    <select
                                        id="district-select-precip"
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
                                    <label htmlFor="month-select-precip">Month:</label>
                                    <select
                                        id="month-select-precip"
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

                            {/* Precipitation Hours Line Chart */}
                            <div className="chart-container">
                                <h4 className="chart-title">
                                    Average Precipitation Hours per Month
                                    {selectedDistrict !== 'All' && ` - ${selectedDistrict}`}
                                    {selectedMonth !== 'All' && ` - ${monthNames[parseInt(selectedMonth) - 1]}`}
                                </h4>
                                <ResponsiveContainer width="100%" height={400}>
                                    <LineChart
                                        data={yearlyHoursData}
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
                                            label={{ value: 'Precipitation Hours', angle: -90, position: 'insideLeft', style: { fontSize: '14px', fontWeight: 600 } }}
                                            tick={{ fontSize: 12 }}
                                            stroke="#6b7280"
                                        />
                                        <Tooltip content={(props) => <CustomLineTooltip {...props} unit="hours" />} />
                                        <Legend wrapperStyle={{ paddingTop: '20px' }} />
                                        <Line
                                            type="monotone"
                                            dataKey="precipitation_hours"
                                            stroke="#2B62C4"
                                            strokeWidth={3}
                                            dot={{ fill: '#2B62C4', r: 5 }}
                                            activeDot={{ r: 7 }}
                                            name="Avg. Precipitation Hours"
                                        />
                                    </LineChart>
                                </ResponsiveContainer>
                            </div>

                            {/* Precipitation Sum Line Chart */}
                            <div className="chart-container">
                                <h4 className="chart-title">
                                    Average Precipitation Sum per Month
                                    {selectedDistrict !== 'All' && ` - ${selectedDistrict}`}
                                    {selectedMonth !== 'All' && ` - ${monthNames[parseInt(selectedMonth) - 1]}`}
                                </h4>
                                <ResponsiveContainer width="100%" height={400}>
                                    <LineChart
                                        data={yearlySumData}
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
                                            label={{ value: 'Precipitation Sum (mm)', angle: -90, position: 'insideLeft', style: { fontSize: '14px', fontWeight: 600 } }}
                                            tick={{ fontSize: 12 }}
                                            stroke="#6b7280"
                                        />
                                        <Tooltip content={(props) => <CustomLineTooltip {...props} unit="mm" />} />
                                        <Legend wrapperStyle={{ paddingTop: '20px' }} />
                                        <Line
                                            type="monotone"
                                            dataKey="precipitation_sum_mm"
                                            stroke="#10B981"
                                            strokeWidth={3}
                                            dot={{ fill: '#10B981', r: 5 }}
                                            activeDot={{ r: 7 }}
                                            name="Avg. Precipitation Sum"
                                        />
                                    </LineChart>
                                </ResponsiveContainer>
                            </div>

                            {/* Summary Stats */}
                            <div className="summary-stats">
                                <div className="stat-box">
                                    <div className="stat-icon">💧</div>
                                    <div className="stat-info">
                                        <div className="stat-label">Avg. Hours/Month</div>
                                        <div className="stat-value">
                                            {(yearlyHoursData.reduce((sum, d) => sum + d.precipitation_hours, 0) / yearlyHoursData.length).toFixed(1)}h
                                        </div>
                                    </div>
                                </div>
                                <div className="stat-box">
                                    <div className="stat-icon">🌧️</div>
                                    <div className="stat-info">
                                        <div className="stat-label">Avg. Precipitation/Month</div>
                                        <div className="stat-value">
                                            {(yearlySumData.reduce((sum, d) => sum + d.precipitation_sum_mm, 0) / yearlySumData.length).toFixed(1)}mm
                                        </div>
                                    </div>
                                </div>
                                <div className="stat-box">
                                    <div className="stat-icon">📊</div>
                                    <div className="stat-info">
                                        <div className="stat-label">Total Districts</div>
                                        <div className="stat-value">{districts.length}</div>
                                    </div>
                                </div>
                                <div className="stat-box">
                                    <div className="stat-icon">📅</div>
                                    <div className="stat-info">
                                        <div className="stat-label">Years Analyzed</div>
                                        <div className="stat-value">{yearlyHoursData.length}</div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </>
                )}
            </div>
        </div>
    );
};

export default Precipitation;
