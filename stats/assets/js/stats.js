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

        // Notify chart views so canvas-based text can be redrawn for the active theme.
        window.dispatchEvent(new CustomEvent('rocketpool-theme-changed', {
            detail: {
                theme: this.theme,
                resolvedTheme: this.resolvedTheme
            }
        }));
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

/**
 * Width Manager
 * Handles wide view toggle with localStorage persistence
 */
class WidthManager {
    constructor() {
        this.init();
    }

    init() {
        const storedWidth = localStorage.getItem('rocketpool-wideview');
        const isWideView = storedWidth === 'true';

        if (isWideView) {
            document.body.classList.add('wide-view');
        }

        this.setupWidthToggle();
    }

    setupWidthToggle() {
        const widthToggle = document.getElementById('wide-view-toggle');
        if (!widthToggle) {
            console.warn('Wide view toggle button not found');
            return;
        }

        this.updateWidthToggleState();

        widthToggle.addEventListener('click', (e) => {
            e.stopPropagation();
            this.toggleWideView();
        });
    }

    toggleWideView() {
        const isCurrentlyWide = document.body.classList.contains('wide-view');

        if (isCurrentlyWide) {
            document.body.classList.remove('wide-view');
            localStorage.setItem('rocketpool-wideview', 'false');
        } else {
            document.body.classList.add('wide-view');
            localStorage.setItem('rocketpool-wideview', 'true');
        }

        this.updateWidthToggleState();
    }

    updateWidthToggleState() {
        const widthToggle = document.getElementById('wide-view-toggle');
        if (!widthToggle) return;

        const isWide = document.body.classList.contains('wide-view');

        if (isWide) {
            widthToggle.classList.add('active');
            widthToggle.title = 'Wide view enabled. Click to return to centred layout.';
        } else {
            widthToggle.classList.remove('active');
            widthToggle.title = 'Centred layout. Click to expand to full width.';
        }
    }
}

class StatsViewer {
    constructor() {
        this.timeRange = 'all'; // Default: All time
        this.charts = {};
        this.statsData = null;
        this.filteredData = null;
    }

    async init() {
        try {
            this.showLoading();
            await this.loadStatsData();
            this.updateHeaderMeta();
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
                this.updateHeaderMeta();
                this.updateSummaryCards();
                this.renderAllCharts();
            });
        });
    }

    getTimeRangeLabel(range) {
        if (range === 'all') {
            return 'All Time';
        }
        return `${range} Days`;
    }

    getLatestSnapshotDate() {
        if (!this.statsData?.snapshots?.length) {
            return null;
        }
        return this.statsData.snapshots.reduce((latest, snapshot) => {
            if (!snapshot?.date) {
                return latest;
            }
            if (!latest) {
                return snapshot.date;
            }
            return new Date(snapshot.date) > new Date(latest) ? snapshot.date : latest;
        }, null);
    }

    formatLongDate(dateString) {
        const date = new Date(dateString);
        if (Number.isNaN(date.getTime())) {
            return 'Unknown';
        }
        return date.toLocaleDateString('en-GB', {
            day: 'numeric',
            month: 'short',
            year: 'numeric'
        });
    }

    updateHeaderMeta() {
        const updateEl = document.getElementById('meta-update-value');
        const snapshotsEl = document.getElementById('meta-snapshots-value');
        const rangeEl = document.getElementById('meta-range-value');

        if (!updateEl && !snapshotsEl && !rangeEl) {
            return;
        }

        const latestSnapshotDate = this.getLatestSnapshotDate();
        if (updateEl) {
            updateEl.textContent = latestSnapshotDate
                ? `${this.formatLongDate(latestSnapshotDate)} @ 01:00 UTC`
                : 'Daily 01:00 UTC';
        }

        if (snapshotsEl) {
            const totalSnapshots = this.statsData?.snapshots?.length || 0;
            snapshotsEl.textContent = `${totalSnapshots.toLocaleString()} total`;
        }

        if (rangeEl) {
            const rangeLabel = this.getTimeRangeLabel(this.timeRange);
            const visiblePoints = this.filteredData?.length || 0;
            rangeEl.textContent = `${rangeLabel} (${visiblePoints})`;
        }
    }

    updateSummaryCards() {
        if (!this.filteredData || this.filteredData.length === 0) return;

        const latest = this.filteredData[0];
        const previous = this.filteredData[1];

        // Underperforming Nodes
        this.updateCard('underperforming', latest.underperforming_nodes, previous?.underperforming_nodes, false);

        // Underperforming Minipools
        this.updateCard('underperforming-minipools', latest.underperforming_minipools, previous?.underperforming_minipools, false);

        // Zero Performance Nodes
        this.updateCard('zero-performance', latest.zero_performance_nodes, previous?.zero_performance_nodes, false);

        // Zero Performance Minipools
        this.updateCard('zero-performance-minipools', latest.zero_performance_minipools, previous?.zero_performance_minipools, false);

        // Average Performance Score
        this.updateCard('performance', latest.avg_performance_score.toFixed(2) + '%', previous?.avg_performance_score, true);
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

    isDarkMode() {
        return document.body.classList.contains('theme-dark') ||
            (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches &&
             !document.body.classList.contains('theme-light'));
    }

    getChartTheme() {
        const darkMode = this.isDarkMode();
        return {
            isDarkMode: darkMode,
            textColor: darkMode ? '#cbd5e1' : '#374151',
            gridColor: darkMode ? 'rgba(148, 163, 184, 0.16)' : 'rgba(0, 0, 0, 0.05)',
            tooltipBg: darkMode ? 'rgba(15, 23, 42, 0.9)' : 'rgba(0, 0, 0, 0.8)'
        };
    }

    hexToRgba(hex, alpha) {
        const value = hex.replace('#', '');
        const normalized = value.length === 3
            ? value.split('').map(char => char + char).join('')
            : value;
        const r = parseInt(normalized.substring(0, 2), 16);
        const g = parseInt(normalized.substring(2, 4), 16);
        const b = parseInt(normalized.substring(4, 6), 16);
        return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    }

    renderAllCharts() {
        this.renderUnderperformingTrend();
        this.renderLostEthTrend();
        this.renderPerformanceScore();
        this.renderZeroPerformance();
        this.renderPerformanceBandDistribution();
        this.renderUndercollateralised();
    }

    renderUnderperformingTrend() {
        const ctx = document.getElementById('underperformingChart');
        if (this.charts.underperforming) {
            this.charts.underperforming.destroy();
        }

        const data = this.filteredData.slice().reverse(); // Chronological order for charts
        console.log('Rendering underperforming chart with', data.length, 'data points');
        const theme = this.getChartTheme();
        const nodeLineColor = theme.isDarkMode ? '#f87171' : '#ef4444';
        const nodeFillColor = theme.isDarkMode ? 'rgba(248, 113, 113, 0.14)' : 'rgba(239, 68, 68, 0.1)';
        const minipoolLineColor = theme.isDarkMode ? '#fb923c' : '#f97316';
        const minipoolFillColor = theme.isDarkMode ? 'rgba(251, 146, 60, 0.14)' : 'rgba(249, 115, 22, 0.1)';

        this.charts.underperforming = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(s => this.formatDate(s.date)),
                datasets: [
                    {
                        label: 'Underperforming Nodes',
                        data: data.map(s => s.underperforming_nodes),
                        borderColor: nodeLineColor,
                        backgroundColor: nodeFillColor,
                        borderWidth: 2,
                        tension: 0.4,
                        fill: true,
                        yAxisID: 'y'
                    },
                    {
                        label: 'Underperforming Minipools',
                        data: data.map(s => s.underperforming_minipools),
                        borderColor: minipoolLineColor,
                        backgroundColor: minipoolFillColor,
                        borderWidth: 2,
                        tension: 0.4,
                        fill: true,
                        yAxisID: 'y'
                    }
                ]
            },
            options: this.getChartOptions('Count')
        });
    }

    renderLostEthTrend() {
        const ctx = document.getElementById('lostEthChart');
        if (this.charts.lostEth) {
            this.charts.lostEth.destroy();
        }

        const data = this.filteredData.slice().reverse();
        const theme = this.getChartTheme();
        const lineColor = theme.isDarkMode ? '#fbbf24' : '#f59e0b';
        const fillColor = theme.isDarkMode ? 'rgba(251, 191, 36, 0.16)' : 'rgba(245, 158, 11, 0.2)';

        this.charts.lostEth = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(s => this.formatDate(s.date)),
                datasets: [{
                    label: 'Total Lost ETH',
                    data: data.map(s => (s.total_lost_gwei / 1_000_000_000).toFixed(2)),
                    borderColor: lineColor,
                    backgroundColor: fillColor,
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
        const theme = this.getChartTheme();
        const lineColor = theme.isDarkMode ? '#60a5fa' : '#2563eb';
        const fillColor = theme.isDarkMode ? 'rgba(96, 165, 250, 0.14)' : 'rgba(37, 99, 235, 0.1)';

        this.charts.performanceScore = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(s => this.formatDate(s.date)),
                datasets: [
                    {
                        label: 'Network Avg Performance',
                        data: data.map(s => s.avg_performance_score),
                        borderColor: lineColor,
                        backgroundColor: fillColor,
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
        const theme = this.getChartTheme();
        const nodeLineColor = theme.isDarkMode ? '#f87171' : '#dc2626';
        const nodeFillColor = theme.isDarkMode ? 'rgba(248, 113, 113, 0.14)' : 'rgba(220, 38, 38, 0.1)';
        const minipoolLineColor = theme.isDarkMode ? '#fb923c' : '#ea580c';
        const minipoolFillColor = theme.isDarkMode ? 'rgba(251, 146, 60, 0.14)' : 'rgba(234, 88, 12, 0.1)';

        this.charts.zeroPerformance = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(s => this.formatDate(s.date)),
                datasets: [
                    {
                        label: 'Zero Performance Nodes',
                        data: data.map(s => s.zero_performance_nodes),
                        borderColor: nodeLineColor,
                        backgroundColor: nodeFillColor,
                        borderWidth: 2,
                        tension: 0.4,
                        fill: true,
                        yAxisID: 'y'
                    },
                    {
                        label: 'Zero Performance Minipools',
                        data: data.map(s => s.zero_performance_minipools),
                        borderColor: minipoolLineColor,
                        backgroundColor: minipoolFillColor,
                        borderWidth: 2,
                        tension: 0.4,
                        fill: true,
                        yAxisID: 'y'
                    }
                ]
            },
            options: this.getChartOptions('Count')
        });
    }

    renderPerformanceBandDistribution() {
        const ctx = document.getElementById('performanceBandChart');
        if (this.charts.performanceBand) {
            this.charts.performanceBand.destroy();
        }

        const data = this.filteredData.slice().reverse();
        const theme = this.getChartTheme();
        const bandFillAlpha = theme.isDarkMode ? 0.28 : 1;
        const bandBorderWidth = theme.isDarkMode ? 1 : 0;
        const bandColors = theme.isDarkMode ? {
            band0: '#a78bfa',
            band0_50: '#f87171',
            band50_80: '#fb7185',
            band80_90: '#fb923c',
            band90_95: '#fbbf24',
            band95_99_5: '#4ade80',
            band99_5_100: '#22d3ee'
        } : {
            band0: '#7c3aed',
            band0_50: '#ef4444',
            band50_80: '#ec4899',
            band80_90: '#f97316',
            band90_95: '#facc15',
            band95_99_5: '#84cc16',
            band99_5_100: '#10b981'
        };

        this.charts.performanceBand = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(s => this.formatDate(s.date)),
                datasets: [
                    {
                        label: '0% Bro, just get rETH',
                        data: data.map(s => s.perf_band_0 || 0),
                        backgroundColor: this.hexToRgba(bandColors.band0, bandFillAlpha),
                        borderColor: bandColors.band0,
                        borderWidth: bandBorderWidth,
                        fill: true,
                        stack: 'performance',
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 0
                    },
                    {
                        label: '0-50% Absolutely Dreadful',
                        data: data.map(s => s.perf_band_0_50 || 0),
                        backgroundColor: this.hexToRgba(bandColors.band0_50, bandFillAlpha),
                        borderColor: bandColors.band0_50,
                        borderWidth: bandBorderWidth,
                        fill: true,
                        stack: 'performance',
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 0
                    },
                    {
                        label: '50-80% Rather Embarrassing',
                        data: data.map(s => s.perf_band_50_80 || 0),
                        backgroundColor: this.hexToRgba(bandColors.band50_80, bandFillAlpha),
                        borderColor: bandColors.band50_80,
                        borderWidth: bandBorderWidth,
                        fill: true,
                        stack: 'performance',
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 0
                    },
                    {
                        label: '80-90% Bit Concerning',
                        data: data.map(s => s.perf_band_80_90 || 0),
                        backgroundColor: this.hexToRgba(bandColors.band80_90, bandFillAlpha),
                        borderColor: bandColors.band80_90,
                        borderWidth: bandBorderWidth,
                        fill: true,
                        stack: 'performance',
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 0
                    },
                    {
                        label: '90-95% Needs Attention',
                        data: data.map(s => s.perf_band_90_95 || 0),
                        backgroundColor: this.hexToRgba(bandColors.band90_95, bandFillAlpha),
                        borderColor: bandColors.band90_95,
                        borderWidth: bandBorderWidth,
                        fill: true,
                        stack: 'performance',
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 0
                    },
                    {
                        label: '95-99.5% Acceptable',
                        data: data.map(s => s.perf_band_95_99_5 || 0),
                        backgroundColor: this.hexToRgba(bandColors.band95_99_5, bandFillAlpha),
                        borderColor: bandColors.band95_99_5,
                        borderWidth: bandBorderWidth,
                        fill: true,
                        stack: 'performance',
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 0
                    },
                    {
                        label: '99.5-100% Excellent',
                        data: data.map(s => s.perf_band_99_5_100 || 0),
                        backgroundColor: this.hexToRgba(bandColors.band99_5_100, bandFillAlpha),
                        borderColor: bandColors.band99_5_100,
                        borderWidth: bandBorderWidth,
                        fill: true,
                        stack: 'performance',
                        tension: 0.4,
                        pointRadius: 0,
                        pointHoverRadius: 0
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                plugins: {
                    legend: {
                        display: true,
                        position: 'top',
                        labels: {
                            usePointStyle: true,
                            padding: 15,
                            font: {
                                size: 11,
                                family: 'Inter, sans-serif'
                            },
                            color: theme.textColor
                        }
                    },
                    tooltip: {
                        backgroundColor: theme.tooltipBg,
                        padding: 12,
                        titleFont: {
                            size: 13,
                            family: 'Inter, sans-serif'
                        },
                        bodyFont: {
                            size: 12,
                            family: 'Inter, sans-serif'
                        },
                        cornerRadius: 6,
                        callbacks: {
                            footer: function(tooltipItems) {
                                let total = 0;
                                tooltipItems.forEach(item => {
                                    total += item.parsed.y;
                                });
                                return '\nTotal Active: ' + total.toLocaleString();
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        stacked: true,
                        grid: {
                            display: false
                        },
                        ticks: {
                            maxRotation: 45,
                            minRotation: 45,
                            font: {
                                size: 11,
                                family: 'Inter, sans-serif'
                            },
                            color: theme.textColor
                        }
                    },
                    y: {
                        stacked: true,
                        grid: {
                            color: theme.gridColor
                        },
                        ticks: {
                            font: {
                                size: 11,
                                family: 'Inter, sans-serif'
                            },
                            color: theme.textColor
                        },
                        title: {
                            display: true,
                            text: 'Active Minipools',
                            font: {
                                size: 12,
                                family: 'Inter, sans-serif',
                                weight: '600'
                            },
                            color: theme.textColor
                        }
                    }
                }
            }
        });
    }

    renderUndercollateralised() {
        const ctx = document.getElementById('undercollateralisedChart');
        if (this.charts.undercollateralised) {
            this.charts.undercollateralised.destroy();
        }

        const data = this.filteredData.slice().reverse();
        const theme = this.getChartTheme();
        const lineColor = theme.isDarkMode ? '#fb923c' : '#ea580c';
        const fillColor = theme.isDarkMode ? 'rgba(251, 146, 60, 0.14)' : 'rgba(234, 88, 12, 0.1)';

        this.charts.undercollateralised = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.map(s => this.formatDate(s.date)),
                datasets: [{
                    label: 'Below 31.9 ETH',
                    data: data.map(s => s.below_31_9_eth),
                    borderColor: lineColor,
                    backgroundColor: fillColor,
                    borderWidth: 2,
                    tension: 0.4,
                    fill: true
                }]
            },
            options: this.getChartOptions('Number of Validators')
        });
    }

    getChartOptions(yAxisLabel, yAxisConfig = {}) {
        const theme = this.getChartTheme();

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
                        },
                        color: theme.textColor
                    }
                },
                tooltip: {
                    backgroundColor: theme.tooltipBg,
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
                        },
                        color: theme.textColor
                    }
                },
                y: {
                    ...yAxisConfig,
                    grid: {
                        color: theme.gridColor
                    },
                    ticks: {
                        font: {
                            size: 11,
                            family: 'Inter, sans-serif'
                        },
                        color: theme.textColor
                    },
                    title: {
                        display: true,
                        text: yAxisLabel,
                        font: {
                            size: 12,
                            family: 'Inter, sans-serif',
                            weight: '600'
                        },
                        color: theme.textColor
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
    const widthManager = new WidthManager();

    // Then initialize stats viewer
    const viewer = new StatsViewer();
    viewer.init();

    // Re-render charts when theme changes so tick/legend text colors update immediately.
    window.addEventListener('rocketpool-theme-changed', () => {
        if (!viewer.filteredData || viewer.filteredData.length === 0) {
            return;
        }
        viewer.renderAllCharts();
    });
});
