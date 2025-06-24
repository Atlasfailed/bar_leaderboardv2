// Common navigation functionality
class Navigation {
    constructor() {
        this.init();
    }

    init() {
        this.setupMobileMenu();
        this.setActiveNavLink();
    }

    setupMobileMenu() {
        const toggle = document.querySelector('.nav-toggle');
        const menu = document.querySelector('.nav-menu');
        
        if (toggle && menu) {
            toggle.addEventListener('click', () => {
                menu.classList.toggle('active');
            });

            // Close menu when clicking a link
            menu.addEventListener('click', (e) => {
                if (e.target.classList.contains('nav-link')) {
                    menu.classList.remove('active');
                }
            });

            // Close menu when clicking outside
            document.addEventListener('click', (e) => {
                if (!toggle.contains(e.target) && !menu.contains(e.target)) {
                    menu.classList.remove('active');
                }
            });
        }
    }

    setActiveNavLink() {
        const currentPath = window.location.pathname;
        const navLinks = document.querySelectorAll('.nav-link');
        
        navLinks.forEach(link => {
            const linkPath = new URL(link.href).pathname;
            if (linkPath === currentPath || 
                (currentPath === '/' && linkPath === '/') ||
                (currentPath.includes('nation-rankings') && linkPath.includes('nation-rankings'))) {
                link.classList.add('active');
            } else {
                link.classList.remove('active');
            }
        });
    }
}

// Common utility functions
const Utils = {
    formatScore: (score) => {
        const scoreClass = score > 0 ? 'positive' : (score < 0 ? 'negative' : '');
        const scoreSign = score > 0 ? '+' : '';
        return { scoreClass, scoreSign, value: score };
    },

    showMessage: (container, message, isError = false) => {
        const className = isError ? 'message error' : 'message';
        container.innerHTML = `<tr><td colspan="3" class="${className}">${message}</td></tr>`;
    },

    debounce: (func, wait) => {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    }
};

// Initialize navigation when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new Navigation();
});
