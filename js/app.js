// ===================================
// SECTION LOADER
// ===================================
const sections = [
    'overview',
    'acquisition',
    'architecture',
    'transformation',
    'feature-engineering',
    'aggregation',
    'impact',
    'reproduction',
    'legal'
];

async function loadSections() {
    const mainContent = document.querySelector('main');

    for (const sectionName of sections) {
        try {
            const response = await fetch(`sections/${sectionName}.html`);
            if (response.ok) {
                const html = await response.text();
                const container = document.getElementById(`${sectionName}-container`);
                if (container) {
                    container.innerHTML = html;
                }
            }
        } catch (error) {
            console.error(`Failed to load section: ${sectionName}`, error);
        }
    }

    // Initialize features after sections are loaded
    initializeFeatures();
}

// ===================================
// FEATURE INITIALIZATION
// ===================================
function initializeFeatures() {
    initDarkMode();
    initStickyHeader();
    initLeftRail();
    initRouteScrollbar();
}

// ===================================
// 1. DARK MODE
// ===================================
function initDarkMode() {
    const themeToggle = document.getElementById('theme-toggle');
    const html = document.documentElement;

    // Check localStorage or system preference
    const savedTheme = localStorage.getItem('theme');
    const systemDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    if (savedTheme === 'dark' || (!savedTheme && systemDark)) {
        html.classList.add('dark');
    }

    if (themeToggle) {
        themeToggle.addEventListener('click', () => {
            html.classList.toggle('dark');
            localStorage.setItem('theme', html.classList.contains('dark') ? 'dark' : 'light');
        });
    }
}

// ===================================
// 2. STICKY HEADER
// ===================================
function initStickyHeader() {
    const dataToggle = document.getElementById('data-toggle');
    const toggleCircle = document.getElementById('toggle-circle');
    const stateRaw = document.getElementById('state-raw');
    const stateProcessed = document.getElementById('state-processed');
    const rowsValue = document.getElementById('rows-value');
    const sizeValue = document.getElementById('size-value');
    const transformationSection = document.getElementById('transformation');

    let isProcessed = false;
    let autoSwitchEnabled = true;

    // Toggle Button Logic
    if (dataToggle) {
        dataToggle.addEventListener('click', () => {
            toggleDataState();
            autoSwitchEnabled = false;
        });
    }

    function toggleDataState() {
        isProcessed = !isProcessed;
        updateHeaderUI();
    }

    function updateHeaderUI() {
        if (isProcessed) {
            toggleCircle?.classList.add('translate-x-6');
            stateRaw?.classList.replace('text-uber-black', 'text-uber-gray500');
            stateRaw?.classList.replace('dark:text-white', 'text-uber-gray500');
            stateProcessed?.classList.replace('text-uber-gray500', 'text-uber-black');
            stateProcessed?.classList.add('dark:text-white');
            if (rowsValue) rowsValue.textContent = '1.02B';
            if (sizeValue) sizeValue.textContent = '250MB';
        } else {
            toggleCircle?.classList.remove('translate-x-6');
            stateRaw?.classList.replace('text-uber-gray500', 'text-uber-black');
            stateRaw?.classList.add('dark:text-white');
            stateProcessed?.classList.replace('text-uber-black', 'text-uber-gray500');
            stateProcessed?.classList.remove('dark:text-white');
            if (rowsValue) rowsValue.textContent = '1.4B';
            if (sizeValue) sizeValue.textContent = '70GB';
        }
    }

    function checkHeaderState() {
        if (!autoSwitchEnabled || !transformationSection) return;

        const rect = transformationSection.getBoundingClientRect();
        const triggerPoint = window.innerHeight * 0.5;

        if (rect.top < triggerPoint && !isProcessed) {
            isProcessed = true;
            updateHeaderUI();
        } else if (rect.top >= triggerPoint && isProcessed) {
            isProcessed = false;
            updateHeaderUI();
        }
    }

    window.addEventListener('scroll', checkHeaderState);
}

// ===================================
// 3. LEFT RAIL & SCROLL SPY
// ===================================
function initLeftRail() {
    const railNav = document.getElementById('rail-nav');
    const railTrack = document.getElementById('rail-track');

    if (!railNav || !railTrack) return;

    const navItems = {};

    // Create navigation items for each section
    sections.forEach(sectionId => {
        const section = document.getElementById(sectionId);
        if (!section) return;

        const heading = section.querySelector('h1, h2');
        const label = heading ? heading.textContent.trim() : sectionId;

        const item = document.createElement('div');
        item.className = 'flex items-center gap-3 cursor-pointer group/item';
        item.innerHTML = `
            <div class="w-3 h-3 rounded-full border-2 border-uber-gray300 dark:border-uber-darkborder bg-white transition-all duration-300"></div>
            <span class="text-sm font-medium text-uber-gray600 opacity-0 group-hover/item:opacity-100 transition-opacity duration-300">${label}</span>
        `;

        item.addEventListener('click', () => {
            section.scrollIntoView({ behavior: 'smooth' });
        });

        railNav.appendChild(item);
        navItems[sectionId] = {
            dot: item.querySelector('div'),
            label: item.querySelector('span'),
            section: section
        };
    });

    function updateNavigation() {
        const scrollY = window.scrollY;
        const viewportHeight = window.innerHeight;

        let activeSectionId = null;

        Object.keys(navItems).forEach(id => {
            const section = navItems[id].section;
            const rect = section.getBoundingClientRect();
            if (rect.top < viewportHeight * 0.5 && rect.bottom > 0) {
                activeSectionId = id;
            }
        });

        if (activeSectionId) {
            Object.keys(navItems).forEach(id => {
                const isActive = id === activeSectionId;
                const item = navItems[id];

                if (isActive) {
                    item.dot.classList.add('bg-uber-black', 'dark:bg-uber-white', 'scale-125');
                    item.dot.classList.remove('bg-white', 'border-uber-gray300');
                    item.label.classList.add('text-uber-black', 'dark:text-white', 'opacity-100');
                    item.label.classList.remove('text-uber-gray600', 'opacity-0');
                } else {
                    item.dot.classList.remove('bg-uber-black', 'dark:bg-uber-white', 'scale-125');
                    item.dot.classList.add('bg-white', 'border-uber-gray300');
                    item.label.classList.remove('text-uber-black', 'dark:text-white', 'opacity-100');
                    item.label.classList.add('text-uber-gray600', 'opacity-0');
                }
            });

            const sectionIds = Object.keys(navItems);
            const activeIndex = sectionIds.indexOf(activeSectionId);
            const progressPercent = (activeIndex / (sectionIds.length - 1)) * 100;
            railTrack.style.height = `${progressPercent}%`;
        }

        updateRouteScrollbar();
    }

    window.addEventListener('scroll', updateNavigation);
    updateNavigation();
}

// ===================================
// 4. ROUTE SCROLLBAR
// ===================================
function initRouteScrollbar() {
    // Initialization handled in updateRouteScrollbar
}

function updateRouteScrollbar() {
    const routeScrollbar = document.getElementById('route-scrollbar');
    const carThumb = document.getElementById('car-thumb');

    if (!routeScrollbar || !carThumb) return;

    const scrollY = window.scrollY;
    const viewportHeight = window.innerHeight;
    const docHeight = document.documentElement.scrollHeight;

    const scrollPercent = scrollY / (docHeight - viewportHeight);
    const trackHeight = routeScrollbar.clientHeight;
    const carPos = scrollPercent * (trackHeight - 32);
    carThumb.style.top = `${carPos}px`;
}

// ===================================
// INITIALIZE ON LOAD
// ===================================
document.addEventListener('DOMContentLoaded', loadSections);
