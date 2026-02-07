class RocketPoolDashboard {
  constructor() {
    this.currentPeriod = '7day';
    this.currentThreshold = 80;
    this.reportData = null;
    this.summaryData = null;
    this.scanData = null;
    this.poapData = null;
    this.currentView = 'main';
    this.currentNodeAddress = null;
    this.showZeroPerformance = true;
    this.excludeBackUpValidators = false;
    this.showOnlyFusakaDeaths = false;
    this.showOnlyBelow32Eth = false;

    // New filtering and pagination
    this.currentSort = 'total-lost-desc';
    this.searchQuery = '';
    this.currentPage = 1;
    this.itemsPerPage = 50;
    this.filteredNodes = [];
    this.scanDataPromise = null;
    this.poapDataPromise = null;
    this.supplementaryLoadScheduled = false;

    // Theme management
    this.theme = 'system'; // 'system', 'light', 'dark'
    this.resolvedTheme = 'light'; // The actual theme being displayed

    // Fusaka hard fork datetime constant
    this.FUSAKA_DATETIME = '2025-12-03T21:49:11';
    this.FUSAKA_EPOCH = 411392;

    // Notes system
    this.notesData = {};
    this.currentUsername = null;
    this.currentNoteNode = null;

    this.init();
  }

  // Security: HTML sanitization to prevent XSS attacks
  sanitizeHtml(str) {
    if (typeof str !== 'string') {
      return String(str);
    }
    
    const temp = document.createElement('div');
    temp.textContent = str;
    return temp.innerHTML;
  }

  // Security: Validate and sanitize data inputs
  validateData(data, type) {
    switch (type) {
      case 'address':
        // Ethereum addresses should be 42 characters starting with 0x
        if (typeof data === 'string' && /^0x[a-fA-F0-9]{40}$/.test(data)) {
          return data.toLowerCase();
        }
        return null;
        
      case 'ensName':
        // ENS names can contain Unicode characters (including emojis)
        // Just validate it's a string ending in .eth
        if (typeof data === 'string' && data.endsWith('.eth') && data.length > 4) {
          return this.sanitizeHtml(data);
        }
        return null;
        
      case 'number':
        // Handle null/undefined/empty values as 0 for numbers
        if (data === null || data === undefined || data === '') {
          return 0;
        }
        const num = parseFloat(data);
        return isNaN(num) ? 0 : num;
        
      case 'percentage':
        const pct = parseFloat(data);
        return isNaN(pct) ? 0 : Math.max(0, Math.min(100, pct));
        
      case 'text':
        if (data === null || data === undefined) return null;
        return this.sanitizeHtml(data);
        
      default:
        if (data === null || data === undefined) return null;
        return this.sanitizeHtml(data);
    }
  }

  // Security: Safe DOM element creation
  createElement(tag, attributes = {}, content = '') {
    const element = document.createElement(tag);
    
    // Set attributes safely
    for (const [key, value] of Object.entries(attributes)) {
      if (key === 'className') {
        element.className = this.sanitizeHtml(value);
      } else if (key === 'textContent') {
        element.textContent = value;
      } else if (key === 'href' && this.isValidUrl(value)) {
        element.setAttribute(key, value);
      } else if (
        key.startsWith('data-') ||
        key.startsWith('aria-') ||
        ['id', 'role', 'tabindex', 'title', 'type'].includes(key)
      ) {
        element.setAttribute(key, this.sanitizeHtml(value));
      }
    }
    
    // Set content safely
    if (content) {
      element.textContent = content;
    }
    
    return element;
  }

  // Security: URL validation
  isValidUrl(url) {
    if (typeof url !== 'string') return false;
    const trimmedUrl = url.trim();
    if (!trimmedUrl) return false;
    if (trimmedUrl.startsWith('#') || trimmedUrl.startsWith('/') || trimmedUrl.startsWith('./') || trimmedUrl.startsWith('../')) {
      return true;
    }
    try {
      const urlObj = new URL(trimmedUrl, window.location.origin);
      return ['http:', 'https:'].includes(urlObj.protocol);
    } catch {
      return false;
    }
  }

  // Check if a node is a "Fusaka Death" (stopped attesting at the hard fork)
  isFusakaDeath(node) {
    if (!node || !node.last_attestation) return false;
    // Check both datetime match (for validators within 120-day window)
    // and status (for persistent tracking after 120-day window)
    return node.last_attestation.datetime === this.FUSAKA_DATETIME ||
           node.last_attestation.status === 'fusaka_death';
  }

  async init() {
    this.initTheme();
    this.initWidthToggle();
    await this.loadSummaryData();
    await this.loadNotesData();
    this.setupDropdowns();
    this.setupEventListeners();
    this.setupToggle();
    this.setupBackUpToggle();
    this.setupFusakaToggle();
    this.setupBelow32EthToggle();
    this.setupThemeToggle();
    this.setupWidthToggle();
    this.setupNoteModal();
    await this.loadReport();
    this.scheduleAutoRefresh();
  }

  async loadSummaryData() {
    try {
      const response = await fetch('summary.json');
      this.summaryData = await response.json();
    } catch (error) {
      console.error('Failed to load summary data:', error);
    }
  }

  async loadScanData() {
    if (this.scanData) return this.scanData;
    if (this.scanDataPromise) return this.scanDataPromise;
    
    this.scanDataPromise = (async () => {
      try {
        const response = await fetch('rocketpool_scan_results.json');
        if (response.ok) {
          this.scanData = await response.json();
        }
      } catch (error) {
        console.warn('Could not load scan data for ENS/withdrawal info:', error);
        this.scanData = null;
      } finally {
        this.scanDataPromise = null;
      }
      return this.scanData;
    })();

    return this.scanDataPromise;
  }

  async loadPoapData() {
    if (this.poapData) return this.poapData;
    if (this.poapDataPromise) return this.poapDataPromise;

    this.poapDataPromise = (async () => {
      try {
        const response = await fetch('poap_results.json');
        if (response.ok) {
          this.poapData = await response.json();
        }
      } catch (error) {
        console.warn('Could not load POAP data:', error);
        this.poapData = null;
      } finally {
        this.poapDataPromise = null;
      }
      return this.poapData;
    })();

    return this.poapDataPromise;
  }

  refreshActiveViewAfterSupplementaryLoad() {
    if (!this.reportData) return;
    if (this.currentView === 'main') {
      this.renderMainReport();
      return;
    }
    if (this.currentView === 'node-detail' && this.currentNodeAddress) {
      this.showNodeDetail(this.currentNodeAddress);
    }
  }

  scheduleSupplementaryDataLoad() {
    if (this.supplementaryLoadScheduled) return;
    this.supplementaryLoadScheduled = true;

    const load = async () => {
      this.supplementaryLoadScheduled = false;
      await Promise.all([this.loadScanData(), this.loadPoapData()]);
      this.refreshActiveViewAfterSupplementaryLoad();
    };

    if (typeof window !== 'undefined' && 'requestIdleCallback' in window) {
      window.requestIdleCallback(load, { timeout: 4000 });
    } else {
      setTimeout(load, 1500);
    }
  }

  async loadNotesData() {
    try {
      const response = await fetch('/api/rp-notes');
      const data = await response.json();
      if (data.success) {
        this.notesData = data.notes;
      }
    } catch (error) {
      console.warn('Could not load notes:', error);
      this.notesData = {};
    }
  }

  getUsername() {
    if (!this.currentUsername) {
      this.currentUsername = localStorage.getItem('rp-notes-username');
      if (!this.currentUsername) {
        this.currentUsername = prompt('Enter your name (for tracking edits):') || 'Anonymous';
        localStorage.setItem('rp-notes-username', this.currentUsername);
      }
    }
    return this.currentUsername;
  }

  async loadReport(period = this.currentPeriod, threshold = this.currentThreshold) {
    this.showLoading();
    
    try {
      const filename = `${period}/performance_${threshold}.json`;
      console.log(`Loading report: ${filename} (period: ${period}, threshold: ${threshold})`);
      // Add cache busting to ensure fresh data
      const response = await fetch(filename + '?t=' + Date.now());
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      this.reportData = await response.json();
      this.currentPeriod = period;
      this.currentThreshold = threshold;

      this.updateUI();
      
      if (this.currentView === 'main') {
        this.renderMainReport();
      } else if (this.currentView === 'node-detail' && this.currentNodeAddress) {
        this.showNodeDetail(this.currentNodeAddress);
      }

      // Load heavy supplemental datasets lazily so the main report paints faster.
      this.scheduleSupplementaryDataLoad();
      
    } catch (error) {
      console.error('Failed to load report:', error);
      console.log(`Error details - period: ${period}, threshold: ${threshold}, type: ${typeof threshold}`);
      const thresholdText = threshold === 'all' ? 'all nodes' : `${threshold}% threshold`;
      this.showError(`Failed to load ${period} report for ${thresholdText}`);
    }
  }

  setupDropdowns() {
    const periodDropdown = document.getElementById('period-dropdown');
    const periodButton = periodDropdown.querySelector('.glass-dropdown-button span');
    const periodItems = periodDropdown.querySelectorAll('.glass-dropdown-item');
    
    periodButton.textContent = this.formatPeriod(this.currentPeriod);
    this.updateSelectedItem(periodItems, this.currentPeriod);
    
    const thresholdDropdown = document.getElementById('threshold-dropdown');
    const thresholdButton = thresholdDropdown.querySelector('.glass-dropdown-button span');
    const thresholdItems = thresholdDropdown.querySelectorAll('.glass-dropdown-item');
    
    if (this.currentThreshold === 'all') {
      thresholdButton.textContent = 'All Nodes';
    } else {
      thresholdButton.textContent = `Under ${this.currentThreshold}%`;
    }
    this.updateSelectedItem(thresholdItems, this.currentThreshold.toString());

    const sortDropdown = document.getElementById('sort-dropdown');
    const sortButton = sortDropdown.querySelector('.glass-dropdown-button span');
    const sortItems = sortDropdown.querySelectorAll('.glass-dropdown-item');
    this.updateSelectedItem(sortItems, this.currentSort);
    const selectedSortItem = Array.from(sortItems).find(item => item.dataset.value === this.currentSort);
    if (selectedSortItem) {
      sortButton.textContent = selectedSortItem.textContent;
    }
  }

  setupEventListeners() {
    this.setupDropdownEvents('period-dropdown', (value) => {
      this.loadReport(value, this.currentThreshold);
    });
    
    this.setupDropdownEvents('threshold-dropdown', (value) => {
      const thresholdValue = value === 'all' ? 'all' : parseInt(value);
      console.log(`Threshold dropdown changed: value=${value}, thresholdValue=${thresholdValue}`);
      this.loadReport(this.currentPeriod, thresholdValue);
    });
    
    this.setupDropdownEvents('sort-dropdown', (value) => {
      this.currentSort = value;
      this.currentPage = 1; // Reset to first page when sorting changes
      this.renderMainReport();
    });
    
    // Search functionality
    const searchInput = document.getElementById('search-input');
    const searchClear = document.getElementById('search-clear');
    
    if (searchInput) {
      searchInput.addEventListener('input', (e) => {
        this.searchQuery = e.target.value.toLowerCase();
        this.currentPage = 1; // Reset to first page when search changes
        this.updateSearchClearVisibility();
        if (this.searchQuery && !this.scanData) {
          this.loadScanData().then(() => this.renderMainReport());
        }
        this.renderMainReport();
      });
      
      searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
          this.clearSearch();
        }
      });
    }
    
    if (searchClear) {
      searchClear.addEventListener('click', () => {
        this.clearSearch();
      });
    }
    
    document.addEventListener('click', (e) => {
      if (!e.target.closest('.glass-dropdown')) {
        document.querySelectorAll('.glass-dropdown').forEach(dropdown => {
          this.closeDropdown(dropdown);
        });
      }
    });
  }

  closeDropdown(dropdown) {
    dropdown.classList.remove('active');
    const button = dropdown.querySelector('.glass-dropdown-button');
    if (button) {
      button.setAttribute('aria-expanded', 'false');
    }
  }

  openDropdown(dropdown) {
    document.querySelectorAll('.glass-dropdown').forEach(otherDropdown => {
      if (otherDropdown !== dropdown) {
        this.closeDropdown(otherDropdown);
      }
    });
    dropdown.classList.add('active');
    const button = dropdown.querySelector('.glass-dropdown-button');
    if (button) {
      button.setAttribute('aria-expanded', 'true');
    }
  }

  moveDropdownFocus(items, currentItem, direction) {
    const itemList = Array.from(items);
    const currentIndex = itemList.indexOf(currentItem);
    if (currentIndex === -1) return;

    let nextIndex = currentIndex + direction;
    if (nextIndex < 0) nextIndex = itemList.length - 1;
    if (nextIndex >= itemList.length) nextIndex = 0;
    itemList[nextIndex].focus();
  }

  selectDropdownItem(dropdown, items, item, buttonText, onSelect) {
    const value = item.dataset.value;
    const text = item.textContent;

    buttonText.textContent = text;
    this.updateSelectedItem(items, value);
    this.closeDropdown(dropdown);
    onSelect(value);
  }

  setupDropdownEvents(dropdownId, onSelect) {
    const dropdown = document.getElementById(dropdownId);
    if (!dropdown) return;
    const button = dropdown.querySelector('.glass-dropdown-button');
    const buttonText = button.querySelector('span');
    const items = dropdown.querySelectorAll('.glass-dropdown-item');
    const content = dropdown.querySelector('.glass-dropdown-content');
    
    button.addEventListener('click', (e) => {
      e.stopPropagation();
      const isOpen = dropdown.classList.contains('active');
      if (isOpen) {
        this.closeDropdown(dropdown);
      } else {
        this.openDropdown(dropdown);
      }
    });

    button.addEventListener('keydown', (e) => {
      if (!['ArrowDown', 'ArrowUp', 'Enter', ' '].includes(e.key)) return;
      e.preventDefault();
      this.openDropdown(dropdown);
      const selected = dropdown.querySelector('.glass-dropdown-item.selected');
      (selected || items[0])?.focus();
    });
    
    items.forEach(item => {
      item.addEventListener('click', (e) => {
        e.stopPropagation();
        this.selectDropdownItem(dropdown, items, item, buttonText, onSelect);
      });

      item.addEventListener('keydown', (e) => {
        if (e.key === 'ArrowDown') {
          e.preventDefault();
          this.moveDropdownFocus(items, item, 1);
          return;
        }
        if (e.key === 'ArrowUp') {
          e.preventDefault();
          this.moveDropdownFocus(items, item, -1);
          return;
        }
        if (e.key === 'Home') {
          e.preventDefault();
          items[0]?.focus();
          return;
        }
        if (e.key === 'End') {
          e.preventDefault();
          items[items.length - 1]?.focus();
          return;
        }
        if (e.key === 'Escape') {
          e.preventDefault();
          this.closeDropdown(dropdown);
          button.focus();
          return;
        }
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          this.selectDropdownItem(dropdown, items, item, buttonText, onSelect);
        }
      });
    });

    content?.addEventListener('keydown', (e) => {
      if (e.key !== 'Escape') return;
      e.preventDefault();
      this.closeDropdown(dropdown);
      button.focus();
    });
  }

  syncSwitchState(toggle, isActive) {
    toggle.classList.toggle('active', isActive);
    toggle.setAttribute('aria-checked', isActive ? 'true' : 'false');
  }

  bindToggleLabel(toggle) {
    const label = toggle.closest('.toggle-container')?.querySelector('.toggle-label');
    if (!label) return;
    label.addEventListener('click', () => toggle.click());
  }

  setupToggle() {
    const toggle = document.getElementById('zero-performance-toggle');
    if (!toggle) return;
    this.syncSwitchState(toggle, this.showZeroPerformance);
    this.bindToggleLabel(toggle);
    
    toggle.addEventListener('click', (e) => {
      e.stopPropagation();
      this.showZeroPerformance = !this.showZeroPerformance;
      this.syncSwitchState(toggle, this.showZeroPerformance);
      
      // Re-render the current view with the new filter
      if (this.currentView === 'main') {
        this.renderMainReport();
      }
    });
  }

  // Search functionality methods
  clearSearch() {
    const searchInput = document.getElementById('search-input');
    if (searchInput) {
      searchInput.value = '';
      this.searchQuery = '';
      this.currentPage = 1;
      this.updateSearchClearVisibility();
      this.renderMainReport();
    }
  }

  updateSearchClearVisibility() {
    const searchClear = document.getElementById('search-clear');
    if (searchClear) {
      if (this.searchQuery.length > 0) {
        searchClear.classList.add('visible');
      } else {
        searchClear.classList.remove('visible');
      }
    }
  }

  // Search filtering method
  filterNodesBySearch(nodes) {
    if (!this.searchQuery) return nodes;
    
    return nodes.filter(node => {
      const nodeAddress = node.node_address.toLowerCase();
      const ensName = this.getNodeEnsName(node.node_address);
      const withdrawalInfo = this.getNodeWithdrawalInfo(node.node_address);
      
      // Search in node address
      if (nodeAddress.includes(this.searchQuery)) return true;
      
      // Search in ENS name
      if (ensName && ensName.toLowerCase().includes(this.searchQuery)) return true;
      
      // Search in withdrawal addresses
      if (withdrawalInfo) {
        const primaryAddr = withdrawalInfo.primary_withdrawal_address?.toLowerCase();
        const rplAddr = withdrawalInfo.rpl_withdrawal_address?.toLowerCase();
        const primaryEns = withdrawalInfo.primary_withdrawal_ens?.toLowerCase();
        const rplEns = withdrawalInfo.rpl_withdrawal_ens?.toLowerCase();
        
        if (primaryAddr && primaryAddr.includes(this.searchQuery)) return true;
        if (rplAddr && rplAddr.includes(this.searchQuery)) return true;
        if (primaryEns && primaryEns.includes(this.searchQuery)) return true;
        if (rplEns && rplEns.includes(this.searchQuery)) return true;
      }
      
      return false;
    });
  }

  // Sorting methods
  sortNodes(nodes) {
    const sorted = [...nodes];
    
    switch (this.currentSort) {
      case 'total-lost-desc':
        return sorted.sort((a, b) => {
          const totalLostA = a.total_lost;
          const totalLostB = b.total_lost;
          return totalLostB - totalLostA;
        });
        
      case 'performance-desc':
        return sorted.sort((a, b) => {
          const scoreA = typeof a.performance_score === 'number' ? a.performance_score : -1;
          const scoreB = typeof b.performance_score === 'number' ? b.performance_score : -1;
          return scoreB - scoreA;
        });
        
      case 'performance-asc':
        return sorted.sort((a, b) => {
          const scoreA = typeof a.performance_score === 'number' ? a.performance_score : 101;
          const scoreB = typeof b.performance_score === 'number' ? b.performance_score : 101;
          return scoreA - scoreB;
        });
        
      default:
        return sorted;
    }
  }

  // Pagination methods
  paginateNodes(nodes) {
    const startIndex = (this.currentPage - 1) * this.itemsPerPage;
    const endIndex = startIndex + this.itemsPerPage;
    return nodes.slice(startIndex, endIndex);
  }

  getTotalPages(totalItems) {
    return Math.ceil(totalItems / this.itemsPerPage);
  }

  setupPagination(totalItems) {
    const paginationContainer = document.getElementById('pagination-container');
    const paginationInfo = document.getElementById('pagination-info-text');
    const paginationPages = document.getElementById('pagination-pages');
    const firstBtn = document.getElementById('pagination-first');
    const prevBtn = document.getElementById('pagination-prev');
    const nextBtn = document.getElementById('pagination-next');
    const lastBtn = document.getElementById('pagination-last');
    
    if (!paginationContainer || totalItems <= this.itemsPerPage) {
      paginationContainer?.classList.add('hidden');
      return;
    }

    paginationContainer.classList.remove('hidden');
    
    const totalPages = this.getTotalPages(totalItems);
    const startItem = (this.currentPage - 1) * this.itemsPerPage + 1;
    const endItem = Math.min(this.currentPage * this.itemsPerPage, totalItems);
    
    // Update info text
    if (paginationInfo) {
      paginationInfo.textContent = `Showing ${startItem}-${endItem} of ${totalItems} nodes`;
    }
    
    // Clear and populate page numbers
    if (paginationPages) {
      paginationPages.innerHTML = '';
      
      const maxVisiblePages = 7;
      let startPage = Math.max(1, this.currentPage - Math.floor(maxVisiblePages / 2));
      let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
      
      // Adjust start if we're near the end
      if (endPage - startPage < maxVisiblePages - 1) {
        startPage = Math.max(1, endPage - maxVisiblePages + 1);
      }
      
      for (let i = startPage; i <= endPage; i++) {
        const pageBtn = document.createElement('button');
        pageBtn.className = `pagination-page ${i === this.currentPage ? 'current' : ''}`;
        pageBtn.textContent = i;
        pageBtn.addEventListener('click', () => {
          this.currentPage = i;
          this.renderMainReport();
        });
        paginationPages.appendChild(pageBtn);
      }
    }
    
    // Setup navigation buttons
    const isFirstPage = this.currentPage === 1;
    const isLastPage = this.currentPage === totalPages;
    
    if (firstBtn) {
      firstBtn.disabled = isFirstPage;
      firstBtn.onclick = () => {
        this.currentPage = 1;
        this.renderMainReport();
      };
    }
    
    if (prevBtn) {
      prevBtn.disabled = isFirstPage;
      prevBtn.onclick = () => {
        this.currentPage = Math.max(1, this.currentPage - 1);
        this.renderMainReport();
      };
    }
    
    if (nextBtn) {
      nextBtn.disabled = isLastPage;
      nextBtn.onclick = () => {
        this.currentPage = Math.min(totalPages, this.currentPage + 1);
        this.renderMainReport();
      };
    }
    
    if (lastBtn) {
      lastBtn.disabled = isLastPage;
      lastBtn.onclick = () => {
        this.currentPage = totalPages;
        this.renderMainReport();
      };
    }
  }

  setupBackUpToggle() {
    const toggle = document.getElementById('exclude-backup-toggle');
    if (!toggle) return;
    this.syncSwitchState(toggle, this.excludeBackUpValidators);
    this.bindToggleLabel(toggle);

    toggle.addEventListener('click', (e) => {
      e.stopPropagation();
      this.excludeBackUpValidators = !this.excludeBackUpValidators;
      this.syncSwitchState(toggle, this.excludeBackUpValidators);

      // Re-render the current view with the new filter
      if (this.currentView === 'main') {
        this.renderMainReport();
      }
    });
  }

  setupFusakaToggle() {
    const toggle = document.getElementById('fusaka-deaths-toggle');
    if (!toggle) return;
    this.syncSwitchState(toggle, this.showOnlyFusakaDeaths);
    this.bindToggleLabel(toggle);

    toggle.addEventListener('click', (e) => {
      e.stopPropagation();
      this.showOnlyFusakaDeaths = !this.showOnlyFusakaDeaths;
      this.syncSwitchState(toggle, this.showOnlyFusakaDeaths);

      // Re-render the current view with the new filter
      if (this.currentView === 'main') {
        this.renderMainReport();
      }
    });
  }

  setupBelow32EthToggle() {
    const toggle = document.getElementById('below-32-eth-toggle');
    if (!toggle) return;
    this.syncSwitchState(toggle, this.showOnlyBelow32Eth);
    this.bindToggleLabel(toggle);

    toggle.addEventListener('click', (e) => {
      e.stopPropagation();
      this.showOnlyBelow32Eth = !this.showOnlyBelow32Eth;
      this.syncSwitchState(toggle, this.showOnlyBelow32Eth);

      // Re-render the current view with the new filter
      if (this.currentView === 'main') {
        this.renderMainReport();
      }
    });
  }

  // Theme management methods
  initTheme() {
    console.log('Initializing theme system');
    // Load theme from localStorage or default to system
    const storedTheme = localStorage.getItem('rocketpool-theme');
    if (storedTheme && ['system', 'light', 'dark'].includes(storedTheme)) {
      this.theme = storedTheme;
    }
    console.log('Initial theme:', this.theme);
    
    this.updateResolvedTheme();
    this.applyTheme();
    
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
    
    // Remove existing theme classes from both html and body
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
    
    console.log('Applied theme:', this.theme, 'to DOM. HTML classes:', html.classList.toString());
    console.log('Body classes:', body.classList.toString());
    
    // Force a style recalculation
    document.body.offsetHeight;
    
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
    themeToggle.setAttribute('aria-label', title);
  }

  setupThemeToggle() {
    const themeToggle = document.getElementById('theme-toggle');
    if (!themeToggle) {
      console.warn('Theme toggle button not found in DOM');
      return;
    }
    
    console.log('Setting up theme toggle button');
    themeToggle.addEventListener('click', (e) => {
      e.stopPropagation();
      console.log('Theme toggle clicked, current theme:', this.theme);
      this.cycleTheme();
    });
  }

  cycleTheme() {
    console.log('Cycling theme from:', this.theme);
    // Cycle: system → light → dark → system
    if (this.theme === 'system') {
      this.theme = 'light';
    } else if (this.theme === 'light') {
      this.theme = 'dark';
    } else {
      this.theme = 'system';
    }
    
    console.log('New theme:', this.theme);
    
    // Save to localStorage
    localStorage.setItem('rocketpool-theme', this.theme);
    
    this.updateResolvedTheme();
    this.applyTheme();
  }

  // Wide View management methods
  initWidthToggle() {
    console.log('Initialising wide view system');
    const storedWidth = localStorage.getItem('rocketpool-wideview');
    const isWideView = storedWidth === 'true';

    if (isWideView) {
      document.body.classList.add('wide-view');
    }

    this.updateWidthToggleState();
  }

  setupWidthToggle() {
    const widthToggle = document.getElementById('wide-view-toggle');
    if (!widthToggle) {
      console.warn('Wide view toggle button not found in DOM');
      return;
    }

    console.log('Setting up wide view toggle button');
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
      widthToggle.setAttribute('aria-label', widthToggle.title);
    } else {
      widthToggle.classList.remove('active');
      widthToggle.title = 'Centred layout. Click to expand to full width.';
      widthToggle.setAttribute('aria-label', widthToggle.title);
    }
  }

  updateSelectedItem(items, selectedValue) {
    items.forEach(item => {
      const isSelected = item.dataset.value === selectedValue;
      if (isSelected) {
        item.classList.add('selected');
      } else {
        item.classList.remove('selected');
      }
      item.setAttribute('aria-selected', isSelected ? 'true' : 'false');
      item.setAttribute('tabindex', isSelected ? '0' : '-1');
    });
  }

  updateUI() {
    if (!this.reportData) return;
    
    document.title = `Rocket Pool Performance Report - ${this.formatPeriod(this.currentPeriod)} (${this.currentThreshold}%)`;
    
    const headerInfo = document.getElementById('header-info');
    if (headerInfo) {
      // Security: Use safe DOM manipulation instead of innerHTML
      headerInfo.innerHTML = ''; // Clear existing content
      
      const lastUpdated = this.formatDate(this.reportData.analysis_date);
      const epochRange = this.reportData.start_epoch && this.reportData.end_epoch 
        ? `${this.reportData.start_epoch.toLocaleString()} to ${this.reportData.end_epoch.toLocaleString()}`
        : 'N/A';
      
      // Create last updated element
      const lastUpdatedDiv = this.createElement('div', { className: 'last-updated' });
      lastUpdatedDiv.textContent = `Last updated: ${lastUpdated}`;
      
      // Create analysis info element
      const analysisInfoDiv = this.createElement('div', { className: 'text-muted' });
      
      const periodSpan = this.createElement('strong');
      periodSpan.textContent = 'Analysis Period: ';
      analysisInfoDiv.appendChild(periodSpan);
      analysisInfoDiv.appendChild(document.createTextNode(this.formatPeriod(this.currentPeriod)));
      
      analysisInfoDiv.appendChild(document.createTextNode(' | '));
      
      const epochsSpan = this.createElement('strong');
      epochsSpan.textContent = 'Epochs: ';
      analysisInfoDiv.appendChild(epochsSpan);
      analysisInfoDiv.appendChild(document.createTextNode(`${this.reportData.epochs_analyzed.toLocaleString()} (${epochRange})`));
      
      analysisInfoDiv.appendChild(document.createTextNode(' | '));
      
      const thresholdSpan = this.createElement('strong');
      thresholdSpan.textContent = 'Threshold: ';
      analysisInfoDiv.appendChild(thresholdSpan);
      const thresholdText = this.currentThreshold === 'all' ? 'All Nodes' : `Under ${this.currentThreshold}%`;
      analysisInfoDiv.appendChild(document.createTextNode(thresholdText));
      
      headerInfo.appendChild(lastUpdatedDiv);
      headerInfo.appendChild(analysisInfoDiv);
    }
  }

  renderMainReport() {
    if (!this.reportData || !this.reportData.node_performance_scores) {
      this.showError('No performance data available');
      return;
    }

    this.currentView = 'main';
    
    // Hide summary card for main view
    const summaryCard = document.getElementById('summary-card');
    if (summaryCard) {
      summaryCard.classList.add('hidden');
    }
    
    // Start with all nodes
    let nodes = this.reportData.node_performance_scores.filter(node => node.active_minipools > 0);
    
    // Apply threshold filtering (for non-"all" thresholds)
    if (this.currentThreshold !== 'all') {
      nodes = nodes.filter(node => 
        typeof node.performance_score === 'number' && 
        node.performance_score < this.currentThreshold
      );
    }
    
    // Apply zero performance filter if toggle is off
    if (!this.showZeroPerformance) {
      nodes = nodes.filter(node => node.performance_score > 0);
    }

    // Apply back-up filter if toggle is on
    if (this.excludeBackUpValidators) {
      nodes = nodes.filter(node => !node.is_back_up);
    }

    // Apply Fusaka Deaths filter if toggle is on
    if (this.showOnlyFusakaDeaths) {
      nodes = nodes.filter(node => this.isFusakaDeath(node));
    }

    // Apply Below 32 ETH filter if toggle is on
    if (this.showOnlyBelow32Eth) {
      nodes = nodes.filter(node => {
        return node.validators_below_32_eth && node.validators_below_32_eth > 0;
      });
    }

    // Apply sorting first to get the full ranking
    nodes = this.sortNodes(nodes);
    
    // Add original ranks based on sorted position (BEFORE search filtering)
    nodes.forEach((node, index) => {
      node._originalRank = index + 1;
    });
    
    // Apply search filtering AFTER adding ranks
    nodes = this.filterNodesBySearch(nodes);
    
    // Store filtered nodes before pagination for statistics
    this.filteredNodes = nodes;
    
    // Setup pagination
    this.setupPagination(nodes.length);
    
    // Apply pagination
    const paginatedNodes = this.paginateNodes(nodes);
    
    // Render components
    this.renderStatistics(this.filteredNodes);
    this.renderMainTable(paginatedNodes);
    this.hideLoading();
  }

  renderStatistics(underperformingNodes) {
    const statsGrid = document.getElementById('stats-grid');
    if (!statsGrid) return;

    const zeroScoreNodes = underperformingNodes.filter(n => n.performance_score === 0).length;
    const backUpNodes = underperformingNodes.filter(n => n.is_back_up).length;
    const below32EthNodes = underperformingNodes.filter(n => n.validators_below_32_eth && n.validators_below_32_eth > 0).length;

    // Calculate count and label based on threshold
    let nodeCount, nodeLabel;
    if (this.currentThreshold === 'all') {
      // For "all" report: show total count and change label
      nodeCount = underperformingNodes.length;
      nodeLabel = 'All Nodes';
    } else {
      // For 80/90/95% reports: exclude zero performance nodes
      nodeCount = underperformingNodes.filter(n => n.performance_score > 0).length;
      nodeLabel = 'Underperforming Nodes';
    }

    const totalActiveMinipools = underperformingNodes.reduce((sum, node) => sum + node.active_minipools, 0);
    const totalRewardsLost = underperformingNodes.reduce((sum, node) =>
      sum + node.total_lost, 0);

    // Fusaka Deaths count
    const fusakaDeaths = underperformingNodes.filter(node => this.isFusakaDeath(node)).length;

    // ENS and security statistics
    const nodesWithEns = underperformingNodes.filter(node => this.getNodeEnsName(node.node_address)).length;
    const nodesWithSeparateWithdrawal = underperformingNodes.filter(node => {
      const withdrawalInfo = this.getNodeWithdrawalInfo(node.node_address);
      return withdrawalInfo && (withdrawalInfo.has_different_primary || withdrawalInfo.has_different_rpl);
    }).length;

    // POAP statistics
    const nodesWithPoaps = underperformingNodes.filter(node => {
      const poapInfo = this.getPoapInfo(node.node_address);
      return poapInfo && poapInfo.has_poaps;
    }).length;
    const totalPoaps = underperformingNodes.reduce((sum, node) => {
      const poapInfo = this.getPoapInfo(node.node_address);
      return sum + (poapInfo && poapInfo.has_poaps ? poapInfo.poap_count : 0);
    }, 0);

    // Security: Use safe DOM manipulation instead of innerHTML
    statsGrid.innerHTML = ''; // Clear existing content

    // Create stat cards
    const statCards = [
      { value: nodeCount, label: nodeLabel },
      { value: zeroScoreNodes, label: 'Zero Performance Nodes' },
      { value: totalActiveMinipools, label: 'minipools' },
      { value: fusakaDeaths, label: 'Fusaka Deaths 💀' },
      { value: below32EthNodes, label: 'Below 31.9 ETH ⚠️' },
      { value: this.formatRewards(totalRewardsLost), label: 'Lost ETH', formatted: true }
    ];

    statCards.forEach(stat => {
      const card = this.createElement('div', { className: 'stat-card' });

      const valueElement = this.createElement('p', { className: 'stat-value' });
      // Handle pre-formatted values (like ETH amounts) or numbers
      valueElement.textContent = stat.formatted
        ? stat.value
        : String(this.validateData(stat.value, 'number'));
      
      const labelElement = this.createElement('p');
      labelElement.textContent = this.validateData(stat.label, 'text');
      
      card.appendChild(valueElement);
      card.appendChild(labelElement);
      statsGrid.appendChild(card);
    });
  }

  // Security: Safe table creation method
  createTable(headers, rows, className = 'performance-table') {
    const tableWrapper = this.createElement('div', { className: className });
    const table = this.createElement('table');
    
    // Create table header
    const thead = this.createElement('thead');
    const headerRow = this.createElement('tr');
    headers.forEach(header => {
      const th = this.createElement('th');
      th.textContent = this.validateData(header, 'text');
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);
    
    // Create table body
    const tbody = this.createElement('tbody');
    rows.forEach((rowData, index) => {
      const row = this.createElement('tr', { 
        className: index % 2 === 0 ? 'table-row-even' : 'table-row-odd'
      });
      
      rowData.forEach(cellData => {
        const td = this.createElement('td');
        
        if (cellData && typeof cellData === 'object') {
          if (cellData.element) {
            td.appendChild(cellData.element);
          } else if (cellData.html) {
            // Only for trusted, pre-validated content
            td.innerHTML = cellData.html;
          } else if (cellData.text) {
            td.textContent = this.validateData(cellData.text, 'text');
          }
          
          if (cellData.className) {
            td.className = this.validateData(cellData.className, 'text');
          }
        } else {
          td.textContent = this.validateData(cellData, 'text');
        }
        
        row.appendChild(td);
      });
      
      tbody.appendChild(row);
    });
    
    table.appendChild(tbody);
    tableWrapper.appendChild(table);
    
    return tableWrapper;
  }

  renderMainTable(underperformingNodes) {
    const tableContainer = document.getElementById('performance-table');
    if (!tableContainer) return;

    // Security: Use safe DOM manipulation
    tableContainer.innerHTML = '';

    const headers = [
      'Rank', 'Node Address', 'Total', 'Active', 'Exited',
      'Performance Score', 'Status', 'ULD', 'Last Attestation', 'Earned Rewards (ETH)',
      'Rewards Missed (ETH)', 'Penalty (ETH)', 'Total Lost (ETH)'
    ];

    const rows = underperformingNodes.map((node, index) => {
      const actualRank = node._originalRank || ((this.currentPage - 1) * this.itemsPerPage + index + 1);
      const rewardsMissed = node.total_missed_rewards;
      const penalties = node.total_penalties;
      const totalLost = node.total_lost;
      const ensName = this.validateData(this.getNodeEnsName(node.node_address), 'ensName');
      const validatedAddress = this.validateData(node.node_address, 'address');
      const canOpenNodeDetail = Boolean(validatedAddress);
      
      // Get withdrawal information
      const withdrawalInfo = this.getNodeWithdrawalInfo(validatedAddress);
      
      // Build address display safely with ENS information
      const cellContainer = this.createElement('div', { className: 'address-cell' });
      const container = this.createElement('div', { className: 'address-container' });
      
      // Add node ENS name if exists
      if (ensName) {
        const ensSpan = this.createElement('div', { className: 'ens-name' });
        ensSpan.textContent = ensName;
        container.appendChild(ensSpan);
      }
      
      // Add node address (always present and keyboard-clickable)
      const addrButton = this.createElement('button', {
        className: 'node-address-btn',
        type: 'button'
      });
      addrButton.textContent = canOpenNodeDetail
        ? this.truncateAddress(validatedAddress)
        : 'Unknown';
      addrButton.disabled = !canOpenNodeDetail;
      if (canOpenNodeDetail) {
        addrButton.addEventListener('click', () => {
          this.showNodeDetail(validatedAddress);
        });
      }
      container.appendChild(addrButton);
      
      // Add withdrawal ENS names if they exist and are different from node address
      if (withdrawalInfo) {
        // Check if ETH and RPL withdrawal addresses are the same
        const hasDifferentPrimary = withdrawalInfo.has_different_primary;
        const hasDifferentRpl = withdrawalInfo.has_different_rpl;
        const sameWithdrawalAddress = withdrawalInfo.primary_withdrawal_address && 
                                     withdrawalInfo.rpl_withdrawal_address &&
                                     withdrawalInfo.primary_withdrawal_address.toLowerCase() === withdrawalInfo.rpl_withdrawal_address.toLowerCase();
        
        if (sameWithdrawalAddress && hasDifferentPrimary && hasDifferentRpl) {
          // Both ETH and RPL use the same withdrawal address - show combined
          const combinedDiv = this.createElement('div', { className: 'withdrawal-ens' });
          
          if (withdrawalInfo.primary_withdrawal_ens) {
            // Use ENS name if available
            combinedDiv.textContent = `↳ ETH & RPL: ${withdrawalInfo.primary_withdrawal_ens}`;
          } else {
            // Use truncated address if no ENS
            combinedDiv.textContent = `↳ ETH & RPL: ${this.truncateAddress(withdrawalInfo.primary_withdrawal_address)}`;
          }
          
          // Check if withdrawal address has POAPs
          const withdrawalPoapInfo = this.getPoapInfo(withdrawalInfo.primary_withdrawal_address);
          if (withdrawalPoapInfo && withdrawalPoapInfo.has_poaps) {
            const withdrawalPoapLink = this.createElement('a', {
              className: 'poap-info poap-link withdrawal-poap',
              href: `https://app.poap.xyz/scan/${withdrawalInfo.primary_withdrawal_address}`
            });
            withdrawalPoapLink.setAttribute('target', '_blank');
            withdrawalPoapLink.setAttribute('rel', 'noopener noreferrer');
            withdrawalPoapLink.setAttribute('title', `View ${withdrawalPoapInfo.poap_count} POAP${withdrawalPoapInfo.poap_count > 1 ? 's' : ''} on POAP.xyz`);
            withdrawalPoapLink.textContent = `🏆 ${withdrawalPoapInfo.poap_count} POAP${withdrawalPoapInfo.poap_count > 1 ? 's' : ''}`;
            
            // Create a new line for POAP link
            const poapDiv = this.createElement('div', { className: 'withdrawal-poap-line' });
            poapDiv.appendChild(withdrawalPoapLink);
            
            container.appendChild(combinedDiv);
            container.appendChild(poapDiv);
          } else {
            container.appendChild(combinedDiv);
          }
        } else {
          // Different withdrawal addresses - show separately
          
          // Handle primary (ETH) withdrawal
          if (withdrawalInfo.primary_withdrawal_ens && hasDifferentPrimary) {
            const primaryDiv = this.createElement('div', { className: 'withdrawal-ens' });
            primaryDiv.textContent = `↳ ETH: ${withdrawalInfo.primary_withdrawal_ens}`;
            
            // Check if primary withdrawal address has POAPs
            const primaryPoapInfo = this.getPoapInfo(withdrawalInfo.primary_withdrawal_address);
            if (primaryPoapInfo && primaryPoapInfo.has_poaps) {
              const primaryPoapLink = this.createElement('a', {
                className: 'poap-info poap-link withdrawal-poap',
                href: `https://app.poap.xyz/scan/${withdrawalInfo.primary_withdrawal_address}`
              });
              primaryPoapLink.setAttribute('target', '_blank');
              primaryPoapLink.setAttribute('rel', 'noopener noreferrer');
              primaryPoapLink.setAttribute('title', `View ${primaryPoapInfo.poap_count} POAP${primaryPoapInfo.poap_count > 1 ? 's' : ''} on POAP.xyz`);
              primaryPoapLink.textContent = `🏆 ${primaryPoapInfo.poap_count} POAP${primaryPoapInfo.poap_count > 1 ? 's' : ''}`;
              
              // Create a new line for POAP link
              const primaryPoapDiv = this.createElement('div', { className: 'withdrawal-poap-line' });
              primaryPoapDiv.appendChild(primaryPoapLink);
              
              container.appendChild(primaryDiv);
              container.appendChild(primaryPoapDiv);
            } else {
              container.appendChild(primaryDiv);
            }
          } else if (hasDifferentPrimary && !withdrawalInfo.primary_withdrawal_ens) {
            // Show primary withdrawal address with POAPs if no ENS
            const primaryPoapInfo = this.getPoapInfo(withdrawalInfo.primary_withdrawal_address);
            if (primaryPoapInfo && primaryPoapInfo.has_poaps) {
              const primaryDiv = this.createElement('div', { className: 'withdrawal-ens' });
              primaryDiv.textContent = `↳ ETH: ${this.truncateAddress(withdrawalInfo.primary_withdrawal_address)} | `;
              
              const primaryPoapLink = this.createElement('a', {
                className: 'poap-info poap-link withdrawal-poap',
                href: `https://app.poap.xyz/scan/${withdrawalInfo.primary_withdrawal_address}`
              });
              primaryPoapLink.setAttribute('target', '_blank');
              primaryPoapLink.setAttribute('rel', 'noopener noreferrer');
              primaryPoapLink.setAttribute('title', `View ${primaryPoapInfo.poap_count} POAP${primaryPoapInfo.poap_count > 1 ? 's' : ''} on POAP.xyz`);
              primaryPoapLink.textContent = `🏆 ${primaryPoapInfo.poap_count} POAP${primaryPoapInfo.poap_count > 1 ? 's' : ''}`;
              
              primaryDiv.appendChild(primaryPoapLink);
              container.appendChild(primaryDiv);
            }
          }
          
          // Handle RPL withdrawal
          if (withdrawalInfo.rpl_withdrawal_ens && hasDifferentRpl) {
            const rplDiv = this.createElement('div', { className: 'withdrawal-ens' });
            rplDiv.textContent = `↳ RPL: ${withdrawalInfo.rpl_withdrawal_ens}`;
            
            // Check if RPL withdrawal address has POAPs
            const rplPoapInfo = this.getPoapInfo(withdrawalInfo.rpl_withdrawal_address);
            if (rplPoapInfo && rplPoapInfo.has_poaps) {
              const rplPoapLink = this.createElement('a', {
                className: 'poap-info poap-link withdrawal-poap',
                href: `https://app.poap.xyz/scan/${withdrawalInfo.rpl_withdrawal_address}`
              });
              rplPoapLink.setAttribute('target', '_blank');
              rplPoapLink.setAttribute('rel', 'noopener noreferrer');
              rplPoapLink.setAttribute('title', `View ${rplPoapInfo.poap_count} POAP${rplPoapInfo.poap_count > 1 ? 's' : ''} on POAP.xyz`);
              rplPoapLink.textContent = `🏆 ${rplPoapInfo.poap_count} POAP${rplPoapInfo.poap_count > 1 ? 's' : ''}`;
              
              // Create a new line for POAP link
              const rplPoapDiv = this.createElement('div', { className: 'withdrawal-poap-line' });
              rplPoapDiv.appendChild(rplPoapLink);
              
              container.appendChild(rplDiv);
              container.appendChild(rplPoapDiv);
            } else {
              container.appendChild(rplDiv);
            }
          } else if (hasDifferentRpl && !withdrawalInfo.rpl_withdrawal_ens) {
            // Show RPL withdrawal address with POAPs if no ENS
            const rplPoapInfo = this.getPoapInfo(withdrawalInfo.rpl_withdrawal_address);
            if (rplPoapInfo && rplPoapInfo.has_poaps) {
              const rplDiv = this.createElement('div', { className: 'withdrawal-ens' });
              rplDiv.textContent = `↳ RPL: ${this.truncateAddress(withdrawalInfo.rpl_withdrawal_address)} | `;
              
              const rplPoapLink = this.createElement('a', {
                className: 'poap-info poap-link withdrawal-poap',
                href: `https://app.poap.xyz/scan/${withdrawalInfo.rpl_withdrawal_address}`
              });
              rplPoapLink.setAttribute('target', '_blank');
              rplPoapLink.setAttribute('rel', 'noopener noreferrer');
              rplPoapLink.setAttribute('title', `View ${rplPoapInfo.poap_count} POAP${rplPoapInfo.poap_count > 1 ? 's' : ''} on POAP.xyz`);
              rplPoapLink.textContent = `🏆 ${rplPoapInfo.poap_count} POAP${rplPoapInfo.poap_count > 1 ? 's' : ''}`;
              
              rplDiv.appendChild(rplPoapLink);
              container.appendChild(rplDiv);
            }
          }
        }
      }
      
      // Add POAP information if available
      const poapInfo = this.getPoapInfo(validatedAddress);
      if (poapInfo && poapInfo.has_poaps) {
        const poapLink = this.createElement('a', {
          className: 'poap-info poap-link',
          href: `https://app.poap.xyz/scan/${validatedAddress}`
        });
        poapLink.setAttribute('target', '_blank');
        poapLink.setAttribute('rel', 'noopener noreferrer');
        poapLink.setAttribute('title', `View ${poapInfo.poap_count} POAP${poapInfo.poap_count > 1 ? 's' : ''} on POAP.xyz`);
        poapLink.textContent = `🏆 ${poapInfo.poap_count} POAP${poapInfo.poap_count > 1 ? 's' : ''}`;
        container.appendChild(poapLink);
      }

      // Add note icon and title
      const hasNote = canOpenNodeDetail && this.notesData[validatedAddress] && this.notesData[validatedAddress].text;
      const noteData = canOpenNodeDetail ? this.notesData[validatedAddress] : null;

      if (canOpenNodeDetail) { // Always show icon for valid node addresses
        const noteContainer = this.createElement('div', { className: 'note-container' });

        const noteIcon = this.createElement('button', {
          className: hasNote ? 'note-icon has-note' : 'note-icon no-note',
          type: 'button',
          'aria-label': hasNote ? `View or edit note for ${validatedAddress}` : `Add note for ${validatedAddress}`
        });
        noteIcon.textContent = '📝';
        noteIcon.title = hasNote ? 'View/edit note' : 'Add note';
        noteIcon.addEventListener('click', () => {
          this.openNoteModal(validatedAddress);
        });
        noteContainer.appendChild(noteIcon);

        // Add note title if exists
        if (hasNote && noteData.title) {
          const noteTitle = this.createElement('button', {
            className: 'note-title',
            type: 'button'
          });
          noteTitle.textContent = this.sanitizeHtml(noteData.title);
          noteTitle.title = 'Click to view/edit note';
          noteTitle.addEventListener('click', () => {
            this.openNoteModal(validatedAddress);
          });
          noteContainer.appendChild(noteTitle);
        }

        container.appendChild(noteContainer);
      }

      cellContainer.appendChild(container);

      // Create performance score element
      const scoreSpan = this.createElement('span', { 
        className: `performance-score ${this.getScoreClass(node.performance_score)}`
      });
      scoreSpan.textContent = this.formatScore(node.performance_score);

      // Create status element
      const statusSpan = this.createElement('span', {
        className: node.is_back_up ? 'status-back-up' : 'status-down'
      });
      statusSpan.textContent = node.is_back_up ? 'Up' : 'Down';

      // Create ULD (Use Latest Delegate) element
      const uldSpan = this.createElement('span', {
        className: `uld-status uld-${node.uld_status || 'unknown'}`
      });

      if (node.uld_status === 'yes') {
        uldSpan.textContent = 'Yes';
      } else if (node.uld_status === 'no') {
        uldSpan.textContent = 'No';
      } else if (node.uld_status === 'partial' && node.uld_count) {
        uldSpan.textContent = `(${node.uld_count})`;
      } else {
        uldSpan.textContent = '?';
      }

      return [
        String(actualRank),
        { element: cellContainer },
        String(this.validateData(node.total_minipools, 'number')),
        String(this.validateData(node.active_minipools, 'number')),
        String(this.validateData(node.exited_minipools, 'number')),
        { element: scoreSpan },
        { element: statusSpan },
        { element: uldSpan },
        { element: node.is_back_up
            ? this.createStatusUpElement()
            : this.createLastAttestationElement(node.last_attestation)
        },
        this.formatRewards(node.total_earned_rewards),
        { text: this.formatRewards(rewardsMissed), className: 'rewards-missed' },
        { text: this.formatRewards(penalties), className: 'penalties' },
        { text: this.formatRewards(totalLost), className: 'total-lost' }
      ];
    });

    const table = this.createTable(headers, rows);
    tableContainer.appendChild(table);
  }

  showNodeDetail(nodeAddress) {
    this.currentView = 'node-detail';
    this.currentNodeAddress = nodeAddress;

    // Add class to body for node detail styling
    document.body.classList.add('node-detail-view');

    if (!this.scanData && !this.scanDataPromise) {
      this.loadScanData().then(() => this.refreshActiveViewAfterSupplementaryLoad());
    }
    if (!this.poapData && !this.poapDataPromise) {
      this.loadPoapData().then(() => this.refreshActiveViewAfterSupplementaryLoad());
    }

    const nodeData = this.reportData.node_performance_scores.find(node => node.node_address === nodeAddress);
    if (!nodeData) {
      this.showError('Node data not found');
      return;
    }

    this.renderNodeDetail(nodeData);
  }

  renderNodeDetail(nodeData) {
    const summaryCard = document.getElementById('summary-card');
    const statsGrid = document.getElementById('stats-grid');
    const tableContainer = document.getElementById('performance-table');
    
    // Find node in scan data and validate
    const nodeInScan = this.scanData ? this.scanData.find(node => node.node_address === nodeData.node_address) : null;
    const ensName = this.validateData(nodeInScan ? nodeInScan.ens_name : null, 'ensName');
    const validatedAddress = this.validateData(nodeData.node_address, 'address');
    const withdrawalInfo = this.getNodeWithdrawalInfo(validatedAddress);
    const securityLevel = this.getSecurityLevel(validatedAddress, withdrawalInfo);
    
    const nodeDisplayName = ensName ? `${ensName} (${validatedAddress})` : validatedAddress;
    
    // Security: Use safe DOM manipulation for summary card
    summaryCard.classList.remove('hidden');
    summaryCard.innerHTML = ''; // Clear existing content
    
    // Create back button
    const backButton = this.createElement('a', { 
      href: '#',
      className: 'back-button'
    });
    backButton.addEventListener('click', (e) => {
      e.preventDefault();
      this.showMainView();
    });
    backButton.textContent = '← Back to All Nodes';
    
    // Create title
    const title = this.createElement('h2');
    title.textContent = `Node Details: ${nodeDisplayName}`;
    
    if (securityLevel && securityLevel !== 'unknown') {
      const securitySpan = this.createElement('span', {
        className: `security-indicator security-${securityLevel}`
      });
      securitySpan.textContent = securityLevel.toUpperCase();
      title.appendChild(document.createTextNode(' '));
      title.appendChild(securitySpan);
    }
    
    // Create description
    const description = this.createElement('div', { className: 'text-muted' });
    description.textContent = 'Detailed view of all validators owned by this node. Click validator public keys to view on beaconcha.in.';
    
    summaryCard.appendChild(backButton);
    summaryCard.appendChild(title);
    summaryCard.appendChild(description);

    // Security: Use safe DOM manipulation for stats grid
    statsGrid.innerHTML = ''; // Clear existing content
    
    // Create basic stat cards
    const basicStats = [
      { value: nodeData.total_minipools, label: 'Total Minipools' },
      { value: nodeData.active_minipools, label: 'Active Minipools' },
      { value: nodeData.exited_minipools, label: 'Exited Minipools' },
      {
        value: nodeData.total_balance_eth ? nodeData.total_balance_eth.toFixed(2) + ' ETH' : 'N/A',
        label: 'Total Balance',
        subtitle: nodeData.avg_balance_eth ? `Avg: ${nodeData.avg_balance_eth.toFixed(2)} ETH` : null,
        isBalance: true
      }
    ];

    basicStats.forEach(stat => {
      const card = this.createElement('div', { className: 'stat-card' });

      const valueElement = this.createElement('p', { className: 'stat-value' });
      valueElement.textContent = stat.isBalance ? stat.value : String(this.validateData(stat.value, 'number'));

      const labelElement = this.createElement('p');
      labelElement.textContent = this.validateData(stat.label, 'text');

      card.appendChild(valueElement);
      card.appendChild(labelElement);

      // Add subtitle if present
      if (stat.subtitle) {
        const subtitleElement = this.createElement('p', { className: 'stat-subtitle' });
        subtitleElement.textContent = this.validateData(stat.subtitle, 'text');
        card.appendChild(subtitleElement);
      }

      statsGrid.appendChild(card);
    });

    // Create withdrawal section if available
    if (withdrawalInfo) {
      const withdrawalCard = this.createWithdrawalSection(withdrawalInfo);
      statsGrid.appendChild(withdrawalCard);
    }

    // Render validator table
    if (nodeInScan && nodeInScan.minipool_pubkeys) {
      this.renderValidatorTable(nodeData, nodeInScan);
    } else {
      tableContainer.innerHTML = '';
      const errorCard = this.createElement('div', { className: 'glass-card p-4 text-center' });
      const errorText = this.createElement('div', { className: 'text-muted' });
      errorText.textContent = 'Validator details not available - scan data not found';
      errorCard.appendChild(errorText);
      tableContainer.appendChild(errorCard);
    }

    this.hideLoading();
  }

  // Security: Create withdrawal section safely
  createWithdrawalSection(withdrawalInfo) {
    const section = this.createElement('div', { className: 'withdrawal-section' });
    
    const title = this.createElement('div', { className: 'withdrawal-title' });
    title.textContent = '🔐 Withdrawal Addresses';
    section.appendChild(title);
    
    // Primary withdrawal
    const primaryDiv = this.createElement('div');
    const primaryLabel = this.createElement('strong');
    primaryLabel.textContent = 'Primary (ETH): ';
    primaryDiv.appendChild(primaryLabel);
    
    const primaryAddr = this.validateData(withdrawalInfo.primary_withdrawal_address, 'address');
    const primaryEns = this.validateData(withdrawalInfo.primary_withdrawal_ens, 'ensName');
    
    if (primaryEns) {
      const ensSpan = this.createElement('span', { className: 'withdrawal-ens' });
      ensSpan.textContent = primaryEns;
      primaryDiv.appendChild(ensSpan);
      primaryDiv.appendChild(document.createElement('br'));
    }
    
    const addrSpan = this.createElement('span', { className: 'withdrawal-addr' });
    addrSpan.textContent = primaryAddr || 'N/A';
    primaryDiv.appendChild(addrSpan);
    
    section.appendChild(primaryDiv);
    
    // RPL withdrawal
    const rplDiv = this.createElement('div');
    rplDiv.style.marginTop = '8px';
    const rplLabel = this.createElement('strong');
    rplLabel.textContent = 'RPL: ';
    rplDiv.appendChild(rplLabel);
    
    const rplAddr = this.validateData(withdrawalInfo.rpl_withdrawal_address, 'address');
    const rplEns = this.validateData(withdrawalInfo.rpl_withdrawal_ens, 'ensName');
    
    if (rplEns) {
      const ensSpan = this.createElement('span', { className: 'withdrawal-ens' });
      ensSpan.textContent = rplEns;
      rplDiv.appendChild(ensSpan);
      rplDiv.appendChild(document.createElement('br'));
    }
    
    const rplAddrSpan = this.createElement('span', { className: 'withdrawal-addr' });
    rplAddrSpan.textContent = rplAddr || 'N/A';
    rplDiv.appendChild(rplAddrSpan);
    
    section.appendChild(rplDiv);
    
    return section;
  }

  getValidatorBalance(nodeData, validatorStatus, status, pubkey) {
    // If validator is exited or not in database, return N/A
    if (status !== 'Active' || !validatorStatus || !validatorStatus.val_id) {
      return 'N/A';
    }

    // Get per-validator balance from report data
    if (this.reportData.validator_balances && this.reportData.validator_balances[pubkey]) {
      const balanceData = this.reportData.validator_balances[pubkey];
      return balanceData.balance_eth || 0;
    }

    // Fallback to node average if per-validator data not available
    if (nodeData.avg_balance_eth && nodeData.avg_balance_eth > 0) {
      return nodeData.avg_balance_eth;
    }

    return 'N/A';
  }

  renderValidatorTable(nodeData, nodeInScan) {
    const tableContainer = document.getElementById('performance-table');
    
    const validators = nodeInScan.minipool_pubkeys.map((pubkey, index) => {
      const validatedPubkey = this.validateData(pubkey, 'text');
      const validatedMinipoolAddr = this.validateData(nodeInScan.minipool_addresses[index], 'address');
      const validatorStatus = this.reportData.validator_statuses && this.reportData.validator_statuses[pubkey];
      
      let status = 'Unknown';
      if (validatorStatus) {
        const dbStatus = validatorStatus.status;
        if (dbStatus === 'active_ongoing' || dbStatus === 'active_exiting') {
          status = 'Active';
        } else if (dbStatus === 'withdrawal_done' || dbStatus === 'exited_unslashed' || dbStatus === 'exited_slashed') {
          status = 'Exited';
        } else if (dbStatus === 'not_in_database') {
          status = 'Exited (Not in DB)';
        } else {
          status = `Other (${dbStatus})`;
        }
      } else {
        status = index < nodeData.active_minipools ? 'Active' : 'Exited';
      }
      
      // Get balance for this validator
      const balance = this.getValidatorBalance(nodeData, validatorStatus, status, pubkey);

      return {
        index: index + 1,
        pubkey: validatedPubkey,
        minipool_address: validatedMinipoolAddr,
        status: status,
        val_id: validatorStatus ? validatorStatus.val_id : null,
        balance: balance
      };
    });

    // Security: Use safe table creation
    tableContainer.innerHTML = '';

    const headers = ['Index', 'Validator Public Key', 'Minipool Address', 'Validator ID', 'Balance (ETH)', 'Status'];

    const rows = validators.map(validator => {
      // Create secure external link
      const beaconchainUrl = `https://beaconcha.in/validator/${encodeURIComponent(validator.pubkey)}`;
      const pubkeyLink = this.createElement('a', {
        href: beaconchainUrl,
        className: 'validator-address'
      });
      pubkeyLink.setAttribute('target', '_blank');
      pubkeyLink.setAttribute('rel', 'noopener noreferrer');
      pubkeyLink.setAttribute('title', 'View on beaconcha.in');
      pubkeyLink.textContent = validator.pubkey;

      // Create balance element
      const balanceSpan = this.createElement('span', {
        className: typeof validator.balance === 'number' && validator.balance < 31.9
          ? 'balance-warning'
          : 'balance-normal'
      });
      balanceSpan.textContent = typeof validator.balance === 'number'
        ? validator.balance.toFixed(4)
        : validator.balance;

      // Create status element
      const statusSpan = this.createElement('span', {
        className: validator.status === 'Active' ? 'active-status' : 'exited-status'
      });
      statusSpan.textContent = this.validateData(validator.status, 'text');

      return [
        String(validator.index),
        { element: pubkeyLink },
        { text: validator.minipool_address, className: 'node-address break-all' },
        validator.val_id ? String(validator.val_id) : 'N/A',
        { element: balanceSpan },
        { element: statusSpan }
      ];
    });

    const table = this.createTable(headers, rows);
    tableContainer.appendChild(table);
  }

  showMainView() {
    this.currentView = 'main';
    this.currentNodeAddress = null;

    // Remove node detail styling class
    document.body.classList.remove('node-detail-view');

    this.renderMainReport();
  }

  // Helper functions for ENS and withdrawal info
  getNodeEnsName(nodeAddress) {
    if (!this.scanData) return null;
    const node = this.scanData.find(n => n.node_address === nodeAddress);
    return node ? node.ens_name : null;
  }

  getNodeWithdrawalInfo(nodeAddress) {
    if (!this.scanData) return null;
    const node = this.scanData.find(n => n.node_address === nodeAddress);
    if (!node) return null;
    
    return {
      primary_withdrawal_address: node.primary_withdrawal_address,
      primary_withdrawal_ens: node.primary_withdrawal_ens,
      rpl_withdrawal_address: node.rpl_withdrawal_address,
      rpl_withdrawal_ens: node.rpl_withdrawal_ens,
      has_different_primary: node.primary_withdrawal_address && node.primary_withdrawal_address !== nodeAddress,
      has_different_rpl: node.rpl_withdrawal_address && node.rpl_withdrawal_address !== nodeAddress
    };
  }

  getPoapInfo(address) {
    if (!this.poapData || !address) return null;
    const lowerAddress = address.toLowerCase();
    return this.poapData[lowerAddress] || null;
  }

  getSecurityLevel(nodeAddress, withdrawalInfo) {
    if (!withdrawalInfo) return 'unknown';
    
    const hasDifferentPrimary = withdrawalInfo.has_different_primary;
    const hasDifferentRpl = withdrawalInfo.has_different_rpl;
    
    if (hasDifferentPrimary && hasDifferentRpl) {
      return 'high';
    } else if (hasDifferentPrimary || hasDifferentRpl) {
      return 'medium';
    } else {
      return 'low';
    }
  }

  getSecurityIndicator(level, compact = false) {
    if (compact) {
      const tooltips = {
        'high': 'Secure: Separate withdrawal addresses',
        'medium': 'Partial: Some withdrawal addresses separate',
        'low': 'Basic: Node address used for withdrawals',
        'unknown': 'Unknown security level'
      };
      return `<span class="security-compact security-${level}" data-tooltip="${tooltips[level] || ''}"></span>`;
    } else {
      const indicators = {
        'high': '<span class="security-indicator security-high">SECURE</span>',
        'medium': '<span class="security-indicator security-medium">PARTIAL</span>',
        'low': '<span class="security-indicator security-low">BASIC</span>',
        'unknown': ''
      };
      return indicators[level] || '';
    }
  }

  formatNodeWithWithdrawal(nodeAddress, ensName, withdrawalInfo, truncate = false) {
    let nodeDisplay = this.formatAddress(nodeAddress, ensName, truncate);
    
    if (!withdrawalInfo) {
      return nodeDisplay;
    }
    
    let withdrawalDisplay = '';
    
    if (withdrawalInfo.primary_withdrawal_address && 
        withdrawalInfo.primary_withdrawal_address !== nodeAddress) {
      
      const truncatedWithdrawalAddr = truncate ? this.truncateAddress(withdrawalInfo.primary_withdrawal_address) : withdrawalInfo.primary_withdrawal_address;
      
      if (withdrawalInfo.primary_withdrawal_ens) {
        withdrawalDisplay += `<div class="withdrawal-display"><span class="withdrawal-label">Withdrawal:</span> <span class="withdrawal-ens">${withdrawalInfo.primary_withdrawal_ens}</span><br><span class="withdrawal-addr">${truncatedWithdrawalAddr}</span></div>`;
      } else {
        withdrawalDisplay += `<div class="withdrawal-display"><span class="withdrawal-label">Withdrawal:</span> <span class="withdrawal-addr">${truncatedWithdrawalAddr}</span></div>`;
      }
    }
    
    return `<div>${nodeDisplay}${withdrawalDisplay}</div>`;
  }

  formatAddress(address, ensName = null, truncate = false) {
    const displayAddress = truncate ? this.truncateAddress(address) : address;
    if (ensName) {
      return `<div class="address-container"><div class="ens-name">${ensName}</div><div class="node-address">${displayAddress}</div></div>`;
    }
    return `<div class="node-address">${displayAddress}</div>`;
  }

  truncateAddress(address) {
    if (address.length <= 20) return address;
    return `${address.slice(0, 6)}...${address.slice(-4)}`;
  }

  formatWithdrawalAddress(address, ensName, truncate = false) {
    if (!address) return 'N/A';
    
    const displayAddress = truncate ? this.truncateAddress(address) : address;
    if (ensName) {
      return `<div class="address-container"><div class="ens-name">${ensName}</div><div class="withdrawal-addr">${displayAddress}</div></div>`;
    }
    return `<div class="withdrawal-addr">${displayAddress}</div>`;
  }

  getScoreClass(score) {
    if (score === 0) return 'score-zero';
    if (score < 10) return 'score-very-low';
    if (score < 30) return 'score-low';
    return 'score-poor';
  }

  formatScore(score) {
    if (score === 0) return '0.00%';
    return score.toFixed(2) + '%';
  }

  formatRewards(rewards) {
    // Handle zero case
    if (rewards === 0) {
      return '0 ETH';
    }
    
    // Convert gwei to ETH (1 ETH = 1,000,000,000 gwei)
    const ethValue = rewards / 1000000000;
    
    if (ethValue >= 1000) {
      return `${(ethValue / 1000).toFixed(3)}K ETH`;
    } else if (ethValue >= 1) {
      return `${ethValue.toFixed(6)} ETH`;
    } else if (ethValue >= 0.001) {
      return `${ethValue.toFixed(6)} ETH`;
    } else if (ethValue >= 0.000001) {
      return `${(ethValue * 1000).toFixed(3)} mETH`;
    }
    return `${(ethValue * 1000000).toFixed(0)} µETH`;
  }

  formatPeriod(period) {
    const periods = {
      '1day': '1 Day',
      '3day': '3 Days', 
      '7day': '7 Days'
    };
    return periods[period] || period;
  }

  // Security: Safe last attestation element creation
  createStatusUpElement() {
    const span = this.createElement('span', {
      className: 'status-back-up'
    });
    span.textContent = 'Up';
    return span;
  }

  createLastAttestationElement(lastAttestationData) {

    if (!lastAttestationData || lastAttestationData.status === 'no_data') {
      const span = this.createElement('span', { className: 'text-muted' });
      span.textContent = 'No data';
      return span;
    }

    // Handle dynamic "older_than_X_days" statuses
    if (lastAttestationData.status && lastAttestationData.status.startsWith('older_than_')) {
      const span = this.createElement('span', { className: 'attestation-old' });
      // Extract days from status like "older_than_45_days"
      const parts = lastAttestationData.status.split('_');
      if (parts.length >= 3 && !isNaN(parts[2])) {
        span.textContent = `> ${parts[2]} days`;
      } else {
        span.textContent = '> 10 days'; // fallback
      }
      return span;
    }

    if (lastAttestationData.status === 'found' || lastAttestationData.status === 'found_extended' || lastAttestationData.status === 'fusaka_death') {
      try {
        const datetime = new Date(lastAttestationData.datetime);

        // Check if the date is valid
        if (isNaN(datetime.getTime())) {
          throw new Error('Invalid datetime');
        }

        const now = new Date();
        const diffMs = now - datetime;
        const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
        const diffDays = Math.floor(diffHours / 24);

        let timeAgo = '';
        if (diffDays > 0) {
          timeAgo = `${diffDays}d ${diffHours % 24}h ago`;
        } else {
          timeAgo = `${diffHours}h ago`;
        }

        const ageClass = this.getAttestationAgeClass(lastAttestationData.age_epochs);
        const container = this.createElement('div', { className: ageClass });

        // Extended search results look the same as regular results
        // (removed extended search indicator text)

        const dateDiv = this.createElement('div');
        dateDiv.style.fontSize = '0.8rem';

        // Check if this is a Fusaka Death and add skull icon
        const isFusaka = lastAttestationData.datetime === this.FUSAKA_DATETIME;
        if (isFusaka) {
          dateDiv.textContent = '💀 ' + datetime.toLocaleDateString();
          dateDiv.style.fontWeight = 'bold';
        } else {
          dateDiv.textContent = datetime.toLocaleDateString();
        }

        const timeDiv = this.createElement('div');
        timeDiv.style.fontSize = '0.75rem';
        timeDiv.style.color = 'var(--neutral-500)';
        timeDiv.textContent = timeAgo;

        container.appendChild(dateDiv);
        container.appendChild(timeDiv);

        return container;
      } catch (error) {
        // Fallback to simple display
        const span = this.createElement('span', { className: 'text-muted' });
        span.textContent = 'Invalid date';
        return span;
      }
    }
    
    const span = this.createElement('span', { className: 'text-muted' });
    span.textContent = 'Unknown';
    return span;
  }

  formatLastAttestation(lastAttestationData) {
    // Deprecated: Use createLastAttestationElement for security
    const element = this.createLastAttestationElement(lastAttestationData);
    const temp = document.createElement('div');
    temp.appendChild(element);
    return temp.innerHTML;
  }

  getAttestationAgeClass(ageEpochs) {
    if (!ageEpochs) return 'attestation-unknown';
    
    // Epochs: 1 epoch ≈ 6.4 minutes
    // Fresh: < 32 epochs (< 3.4 hours)
    // Recent: < 225 epochs (< 1 day) 
    // Stale: < 675 epochs (< 3 days)
    // Very stale: >= 675 epochs (>= 3 days)
    
    if (ageEpochs < 32) return 'attestation-fresh';
    if (ageEpochs < 225) return 'attestation-recent';
    if (ageEpochs < 675) return 'attestation-stale';
    return 'attestation-very-stale';
  }


  formatDate(dateString) {
    return new Date(dateString).toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZone: 'UTC',
      timeZoneName: 'short'
    });
  }

  showLoading() {
    const tableContainer = document.getElementById('performance-table');
    if (tableContainer) {
      // Security: Use safe DOM manipulation for loading state
      tableContainer.innerHTML = '';
      
      const loadingCard = this.createElement('div', { className: 'glass-card p-4 text-center' });
      
      const spinner = this.createElement('div', { className: 'loading-spinner' });
      spinner.style.margin = '0 auto 16px';
      
      const loadingText = this.createElement('div');
      loadingText.textContent = 'Loading performance data...';
      
      loadingCard.appendChild(spinner);
      loadingCard.appendChild(loadingText);
      tableContainer.appendChild(loadingCard);
    }
  }

  hideLoading() {
    // Loading state is replaced by content
  }

  showError(message) {
    const tableContainer = document.getElementById('performance-table');
    if (tableContainer) {
      // Security: Use safe DOM manipulation for error display
      tableContainer.innerHTML = '';
      
      const errorCard = this.createElement('div', { className: 'glass-card p-4 text-center' });
      errorCard.style.borderColor = 'var(--danger)';
      errorCard.style.background = 'rgba(239, 68, 68, 0.05)';
      
      const errorTitle = this.createElement('div');
      errorTitle.style.color = 'var(--danger)';
      errorTitle.style.fontWeight = '600';
      errorTitle.style.marginBottom = '8px';
      errorTitle.textContent = 'Error';
      
      const errorMessage = this.createElement('div');
      errorMessage.textContent = this.validateData(message, 'text');
      
      errorCard.appendChild(errorTitle);
      errorCard.appendChild(errorMessage);
      tableContainer.appendChild(errorCard);
    }
  }

  // Notes modal methods
  openNoteModal(nodeAddress) {
    const modal = document.getElementById('note-modal');
    const nodeAddrSpan = document.getElementById('note-node-address');
    const viewMode = document.getElementById('note-view-mode');
    const editMode = document.getElementById('note-edit-mode');
    const renderedContent = document.getElementById('note-rendered-content');
    const metadata = document.getElementById('note-metadata');
    const textarea = document.getElementById('note-textarea');
    const titleInput = document.getElementById('note-title-input');
    const viewTitle = document.getElementById('note-view-title');

    // Store current node
    this.currentNoteNode = nodeAddress;

    // Display node address
    const ensName = this.getNodeEnsName(nodeAddress);
    nodeAddrSpan.textContent = ensName || this.truncateAddress(nodeAddress);

    // Get note data
    const noteData = this.notesData[nodeAddress] || { text: '', title: '' };

    // Render note or show empty state
    if (noteData.text) {
      // Show title in view mode
      if (noteData.title) {
        viewTitle.textContent = noteData.title;
        viewTitle.classList.remove('hidden');
      } else {
        viewTitle.classList.add('hidden');
      }

      renderedContent.innerHTML = this.renderMarkdown(noteData.text);
      metadata.textContent = `Last updated: ${new Date(noteData.updated_at).toLocaleString()} by ${this.sanitizeHtml(noteData.updated_by)}`;
    } else {
      viewTitle.classList.add('hidden');
      renderedContent.innerHTML = '<p class="text-muted">No note yet. Click "Edit Note" to add one.</p>';
      metadata.textContent = '';
    }

    // Set textarea and title input values
    textarea.value = noteData.text || '';
    if (titleInput) {
      titleInput.value = noteData.title || '';
    }

    // Show view mode
    viewMode.classList.remove('hidden');
    editMode.classList.add('hidden');

    // Show modal
    modal.classList.remove('hidden');
  }

  renderMarkdown(text) {
    // Basic markdown rendering (converts common patterns)
    let html = this.sanitizeHtml(text);

    // Bold: **text**
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');

    // Italic: *text*
    html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');

    // Line breaks
    html = html.replace(/\n/g, '<br>');

    // Links: [text](url)
    html = html.replace(/\[(.+?)\]\((.+?)\)/g, (match, text, url) => {
      const safeUrl = url.trim();
      if (!this.isValidUrl(safeUrl)) {
        return text;
      }
      return `<a href="${this.sanitizeHtml(safeUrl)}" target="_blank" rel="noopener noreferrer">${text}</a>`;
    });

    return html;
  }

  switchToEditMode() {
    const viewMode = document.getElementById('note-view-mode');
    const editMode = document.getElementById('note-edit-mode');
    viewMode.classList.add('hidden');
    editMode.classList.remove('hidden');
  }

  async saveNote() {
    const textarea = document.getElementById('note-textarea');
    const titleInput = document.getElementById('note-title-input');
    const noteText = textarea.value.trim();
    const noteTitle = titleInput ? titleInput.value.trim() : '';
    const username = this.getUsername();

    if (!noteText) {
      if (confirm('Note is empty. Delete this note?')) {
        await this.deleteNote();
      }
      return;
    }

    const now = new Date().toISOString();
    const isNew = !this.notesData[this.currentNoteNode];

    this.notesData[this.currentNoteNode] = {
      title: noteTitle,
      text: noteText,
      created_at: isNew ? now : (this.notesData[this.currentNoteNode]?.created_at || now),
      updated_at: now,
      updated_by: username
    };

    // Save to server via API
    try {
      const response = await fetch('/api/rp-notes', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ notes: this.notesData })
      });

      const result = await response.json();
      if (result.success) {
        alert('Note saved successfully!');
        this.closeNoteModal();
        this.renderMainReport(); // Refresh to show note icon
      } else {
        alert('Failed to save note.');
      }
    } catch (error) {
      console.error('Error saving note:', error);
      alert('Error saving note: ' + error.message);
    }
  }

  async deleteNote() {
    if (this.notesData[this.currentNoteNode]) {
      delete this.notesData[this.currentNoteNode];

      try {
        const response = await fetch('/api/rp-notes', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ notes: this.notesData })
        });

        const result = await response.json();
        if (result.success) {
          this.closeNoteModal();
          this.renderMainReport();
        }
      } catch (error) {
        console.error('Error deleting note:', error);
        alert('Error deleting note: ' + error.message);
      }
    }
  }

  closeNoteModal() {
    const modal = document.getElementById('note-modal');
    modal.classList.add('hidden');
    this.currentNoteNode = null;
  }

  setupNoteModal() {
    const modalClose = document.getElementById('note-modal-close');
    const editBtn = document.getElementById('note-edit-btn');
    const saveBtn = document.getElementById('note-save-btn');
    const cancelBtn = document.getElementById('note-cancel-btn');
    const deleteBtn = document.getElementById('note-delete-btn');
    const overlay = document.querySelector('#note-modal .modal-overlay');

    if (modalClose) {
      modalClose.addEventListener('click', () => {
        this.closeNoteModal();
      });
    }

    if (editBtn) {
      editBtn.addEventListener('click', () => {
        this.switchToEditMode();
      });
    }

    if (saveBtn) {
      saveBtn.addEventListener('click', () => {
        this.saveNote();
      });
    }

    if (cancelBtn) {
      cancelBtn.addEventListener('click', () => {
        this.closeNoteModal();
      });
    }

    if (deleteBtn) {
      deleteBtn.addEventListener('click', () => {
        if (confirm('Are you sure you want to delete this note?')) {
          this.deleteNote();
        }
      });
    }

    if (overlay) {
      overlay.addEventListener('click', () => {
        this.closeNoteModal();
      });
    }
  }

  scheduleAutoRefresh() {
    // Auto-refresh at 7 minutes past every hour
    const now = new Date();
    const nextRefresh = new Date(now);
    nextRefresh.setMinutes(7, 0, 0);

    if (now.getMinutes() >= 7) {
      nextRefresh.setHours(nextRefresh.getHours() + 1);
    }

    const timeUntilRefresh = nextRefresh.getTime() - now.getTime();

    setTimeout(() => {
      this.loadReport();
      setInterval(() => this.loadReport(), 60 * 60 * 1000); // Every hour
    }, timeUntilRefresh);
  }
}

// Initialize dashboard
let dashboard;
document.addEventListener('DOMContentLoaded', () => {
  dashboard = new RocketPoolDashboard();
});

// ========================================
// SATURN I LAUNCH COUNTDOWN TIMER
// ========================================
(function() {
  'use strict';

  const LAUNCH_DATE = new Date('2026-02-18T00:00:00Z');
  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  // Rotating facts about Saturn I
  const SATURN_FACTS = [
    "💎 Earn staking rewards with rETH whilst keeping capital active in DeFi",
    "🚀 Saturn 1 removes capacity limits so rETH can scale with Ethereum",
    "🛡️ Hold rETH to support decentralisation, earn yield, all simultaneously",
    "⚡ rETH democratises Ethereum staking by lowering barriers to entry",
    "🌊 Liquid staking without compromise through rETH security and flexibility",
    "📈 Saturn 1 boosts capital efficiency making rETH more competitive",
    "🔥 No locks, no limits, rETH keeps your ETH productive 24/7",
    "🌍 rETH is the liquid staking token built on true decentralisation"
  ];

  let currentFactIndex = 0;

  function updateSaturnCountdown() {
    const countdownElement = document.getElementById('saturn-countdown');
    if (!countdownElement) return;

    const now = new Date();
    const diff = LAUNCH_DATE - now;

    if (diff <= 0) {
      countdownElement.textContent = 'LIVE NOW! 🚀';
      countdownElement.style.color = '#047857'; // Success green
      return;
    }

    // Calculate time units
    const days = Math.floor(diff / (1000 * 60 * 60 * 24));
    const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
    const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
    const seconds = Math.floor((diff % (1000 * 60)) / 1000);

    // Format the countdown
    countdownElement.textContent = `${days}d ${hours}h ${minutes}m ${seconds}s`;
  }

  function rotateSaturnFact() {
    const factElement = document.getElementById('saturn-rotating-fact');
    if (!factElement) return;

    // Fade out
    factElement.style.opacity = '0';

    setTimeout(() => {
      // Update fact
      currentFactIndex = (currentFactIndex + 1) % SATURN_FACTS.length;
      factElement.textContent = SATURN_FACTS[currentFactIndex];

      // Fade in
      factElement.style.opacity = '0.85';
    }, 300);
  }

  // Initialize countdown when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() {
      updateSaturnCountdown();
      setInterval(updateSaturnCountdown, 1000);

      if (!prefersReducedMotion) {
        // Rotate facts every 5 seconds
        setInterval(rotateSaturnFact, 5000);
      }
    });
  } else {
    updateSaturnCountdown();
    setInterval(updateSaturnCountdown, 1000);

    if (!prefersReducedMotion) {
      // Rotate facts every 5 seconds
      setInterval(rotateSaturnFact, 5000);
    }
  }
})();
