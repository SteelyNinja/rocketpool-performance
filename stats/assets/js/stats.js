/**
 * Rocket Pool Stats Viewer
 * Renders historical network trends using Chart.js
 */

/**
 * Theme Manager
 * Handles light/dark/system theme switching with localStorage persistence
 */
class ThemeManager {
    constructor() {
        this.theme = 'system'; // 'system', 'light', 'dark'
        this.resolvedTheme = 'light';
        this.init();
    }

    init() {
        // Load theme from localStorage (syncs with main dashboard)
        const storedTheme = localStorage.getItem('rocketpool-theme');
        if (storedTheme && ['system', 'light', 'dark'].includes(storedTheme)) {
            this.theme = storedTheme;
        }

        this.updateResolvedTheme();
        this.applyTheme();
        this.setupThemeToggle();

        // Listen for system theme changes
        const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
        mediaQuery.addEventListener('change', () => {
            if (this.theme === 'system') {
                this.updateResolvedTheme();
                this.applyTheme();
            }
        });
    }

    updateResolvedTheme() {
        if (this.theme === 'system') {
            const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
            this.resolvedTheme = mediaQuery.matches ? 'dark' : 'light';
        } else {
            this.resolvedTheme = this.theme;
        }
    }

    applyTheme() {
        const html = document.documentElement;
        const body = document.body;

        // Remove existing theme classes
        html.classList.remove('theme-light', 'theme-dark');
        body.classList.remove('theme-light', 'theme-dark');

        // Apply theme class if not system
        if (this.theme === 'light') {
            html.classList.add('theme-light');
            body.classList.add('theme-light');
        } else if (this.theme === 'dark') {
            html.classList.add('theme-dark');
            body.classList.add('theme-dark');
        }

        this.updateThemeToggleIcon();
    }

    updateThemeToggleIcon() {
        const themeToggle = document.getElementById('theme-toggle');
        if (!themeToggle) return;

        let icon, title;
        if (this.theme === 'system') {
            icon = '🖥️';
            title = `System theme (currently ${this.resolvedTheme}). Click to switch to light mode.`;
        } else if (this.theme === 'light') {
            icon = '☀️';
            title = 'Light mode. Click to switch to dark mode.';
        } else {
            icon = '🌙';
            title = 'Dark mode. Click to switch to system theme.';
        }

        themeToggle.textContent = icon;
        themeToggle.title = title;
    }

    setupThemeToggle() {
        const themeToggle = document.getElementById('theme-toggle');
        if (!themeToggle) {
            console.warn('Theme toggle button not found');
            return;
        }

        themeToggle.addEventListener('click', (e) => {
            e.stopPropagation();
            this.cycleTheme();
        });
    }

    cycleTheme() {
        // Cycle: system → light → dark → system
        if (this.theme === 'system') {
            this.theme = 'light';
        } else if (this.theme === 'light') {
            this.theme = 'dark';
        } else {
            this.theme = 'system';
        }

        // Save to localStorage (syncs with main dashboard)
        localStorage.setItem('rocketpool-theme', this.theme);

        this.updateResolvedTheme();
        this.applyTheme();
    }
}

class StatsViewer {
    constructor() {
        this.timeRange = '30'; // Default: 30 days
        this.charts = {};
        this.statsData = null;
        this.filteredData = null;
    }

    async init() {
        try {
            this.showLoading();
            await this.loadStatsData();
            this.setupTimeRangeControls();
            this.updateSummaryCards();
            this.renderAllCharts();
            this.hideLoading();
        } catch (error) {
            console.error('Failed to initialise stats viewer:', error);
            this.showError(error.message);
        }
    }

    showLoading() {
        document.getElementById('loading').style.display = 'flex';
    }

    hideLoading() {
        document.getElementById('loading').style.display = 'none';
    }

    showError(message) {
        this.hideLoading();
        const errorEl = document.getElementById('error');
        const errorText = document.getElementById('error-text');
        errorText.textContent = message || 'Failed to load statistics data';
        errorEl.style.display = 'flex';
    }

    async loadStatsData() {
        try {
            const response = await fetch('stats_history.json');
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            this.statsData = await response.json();
            this.filteredData = this.filterDataByTimeRange(this.timeRange);
            console.log(`Loaded ${this.statsData.snapshots.length} snapshots`);
            console.log(`Filtered to ${this.filteredData.length} snapshots for ${this.timeRange} day range`);
            console.log('Filtered dates:', this.filteredData.map(s => s.date));
        } catch (error) {
            console.error('Error loading stats data:', error);
            throw new Error('Failed to load statistics data. Please try again later.');
        }
    }

    filterDataByTimeRange(range) {
        if (!this.statsData || !this.statsData.snapshots) {
            return [];
        }

        const snapshots = [...this.statsData.snapshots].reverse(); // Most recent first

        if (range === 'all') {
            return snapshots;
        }

        const days = parseInt(range);
        return snapshots.slice(0, days);
    }

    setupTimeRangeControls() {
        const buttons = document.querySelectorAll('.range-btn');
        buttons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                // Update active state
                buttons.forEach(b => b.classList.remove('active'));
                e.target.classList.add('active');

                // Update range and re-render
                this.timeRange = e.target.dataset.range;
                this.filteredData = this.filterDataByTimeRange(this.timeRange);
                this.updateSummaryCards();
                this.renderAllCharts();
            });
        });
    }

    updateSummaryCards() {
        if (!this.filteredData || this.filteredData.length === 0) return;

        const latest = this.filteredData[0];
        const previous = this.filteredData[1];

        // Underperforming Nodes
        this.updateCard('underperforming', latest.underperforming_nodes, previous?.underperforming_nodes, false);

        // Average Performance Score
        this.updateCard('performance', latest.avg_performance_score.toFixed(2) + '%', previous?.avg_performance_score, true);

        // Zero Performance Nodes
        this.updateCard('zero-performance', latest.zero_performance_nodes, previous?.zero_performance_nodes, false);
    }

    updateCard(prefix, value, previousValue, higherIsBetter) {
        const valueEl = document.getElementById(`${prefix}-value`);
        const changeEl = document.getElementById(`${prefix}-change`);

        valueEl.textContent = value;

        if (previousValue !== undefined) {
            const currentNum = typeof value === 'string' ? parseFloat(value.replace(/[^0-9.-]+/g, '')) : value;
            const change = currentNum - previousValue;
            const changePercent = previousValue !== 0 ? (change / previousValue * 100).toFixed(1) : 0;

            const isPositive = change > 0;
            const isBeneficial = higherIsBetter ? isPositive : !isPositive;

            changeEl.className = 'card-change';
            if (change === 0) {
                changeEl.classList.add('neutral');
                changeEl.textContent = 'No change';
            } else {
                changeEl.classList.add(isBeneficial ? 'positive' : 'negative');
                const arrow = isPositive ? '↑' : '↓';
                changeEl.textContent = `${arrow} ${Math.abs(change).toFixed(0)} (${Math.abs(changePercent)}%) from previous day`;
            }
        } else {
            changeEl.textContent = 'Insufficient data';
            changeEl.className = 'card-change neutral';
        }
    }

    renderAllCharts() {
        this.renderUnderperformingTrend();
        this.renderLostEthTrend();
        this.renderPerformanceScore();
        this.renderZeroPerformance();
        this.renderUndercollateralised();
    }

    renderUnderperformingTrend() {
        const ctx = document.getElementById('underperformingChart');
        if (this.charts.underperforming) {
            this.charts.underperforming.destroy();
        }

        const data = this.filteredData.slice().reverse(); // Chronological order for charts
        console.log('Rendering underperforming chart with', data.length, 'data points');

        this.charts.underperforming = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(s => this.formatDate(s.date)),
                datasets: [{
                    label: 'Underperforming Nodes',
                    data: data.map(s => s.underperforming_nodes),
                    borderColor: '#ef4444',
                    backgroundColor: 'rgba(239, 68, 68, 0.1)',
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true
                }]
            },
            options: this.getChartOptions('Number of Nodes')
        });
    }

    renderLostEthTrend() {
        const ctx = document.getElementById('lostEthChart');
        if (this.charts.lostEth) {
            this.charts.lostEth.destroy();
        }

        const data = this.filteredData.slice().reverse();

        this.charts.lostEth = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(s => this.formatDate(s.date)),
                datasets: [{
                    label: 'Total Lost ETH',
                    data: data.map(s => (s.total_lost_gwei / 1_000_000_000).toFixed(2)),
                    borderColor: '#f59e0b',
                    backgroundColor: 'rgba(245, 158, 11, 0.2)',
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true
                }]
            },
            options: this.getChartOptions('ETH Lost (7-day)')
        });
    }

    renderPerformanceScore() {
        const ctx = document.getElementById('performanceScoreChart');
        if (this.charts.performanceScore) {
            this.charts.performanceScore.destroy();
        }

        const data = this.filteredData.slice().reverse();

        this.charts.performanceScore = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(s => this.formatDate(s.date)),
                datasets: [
                    {
                        label: 'Network Avg Performance',
                        data: data.map(s => s.avg_performance_score),
                        borderColor: '#2563eb',
                        backgroundColor: 'rgba(37, 99, 235, 0.1)',
                        borderWidth: 2,
                        tension: 0.4,
                        fill: true
                    }
                ]
            },
            options: this.getChartOptions('Performance Score (%)')
        });
    }

    renderZeroPerformance() {
        const ctx = document.getElementById('zeroPerformanceChart');
        if (this.charts.zeroPerformance) {
            this.charts.zeroPerformance.destroy();
        }

        const data = this.filteredData.slice().reverse();

        this.charts.zeroPerformance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(s => this.formatDate(s.date)),
                datasets: [{
                    label: 'Zero Performance Nodes',
                    data: data.map(s => s.zero_performance_nodes),
                    borderColor: '#dc2626',
                    backgroundColor: 'rgba(220, 38, 38, 0.1)',
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true
                }]
            },
            options: this.getChartOptions('Number of Nodes')
        });
    }

    renderUndercollateralised() {
        const ctx = document.getElementById('undercollateralisedChart');
        if (this.charts.undercollateralised) {
            this.charts.undercollateralised.destroy();
        }

        const data = this.filteredData.slice().reverse();

        this.charts.undercollateralised = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(s => this.formatDate(s.date)),
                datasets: [{
                    label: 'Below 31.9 ETH',
                    data: data.map(s => s.below_31_9_eth),
                    borderColor: '#ea580c',
                    backgroundColor: 'rgba(234, 88, 12, 0.1)',
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true
                }]
            },
            options: this.getChartOptions('Number of Validators')
        });
    }

    getChartOptions(yAxisLabel, yAxisConfig = {}) {
        return {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        usePointStyle: true,
                        padding: 15,
                        font: {
                            size: 12,
                            family: 'Inter, sans-serif'
                        }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(0, 0, 0, 0.8)',
                    padding: 12,
                    titleFont: {
                        size: 13,
                        family: 'Inter, sans-serif'
                    },
                    bodyFont: {
                        size: 12,
                        family: 'Inter, sans-serif'
                    },
                    cornerRadius: 6
                }
            },
            scales: {
                x: {
                    grid: {
                        display: false
                    },
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        font: {
                            size: 11,
                            family: 'Inter, sans-serif'
                        }
                    }
                },
                y: {
                    ...yAxisConfig,
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    },
                    ticks: {
                        font: {
                            size: 11,
                            family: 'Inter, sans-serif'
                        }
                    },
                    title: {
                        display: true,
                        text: yAxisLabel,
                        font: {
                            size: 12,
                            family: 'Inter, sans-serif',
                            weight: '600'
                        }
                    }
                }
            },
            interaction: {
                mode: 'index',
                intersect: false
            }
        };
    }

    formatDate(dateString) {
        const date = new Date(dateString);
        const month = date.toLocaleDateString('en-GB', { month: 'short' });
        const day = date.getDate();
        return `${month} ${day}`;
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    // Initialize theme manager first
    const themeManager = new ThemeManager();

    // Then initialize stats viewer
    const viewer = new StatsViewer();
    viewer.init();
});
