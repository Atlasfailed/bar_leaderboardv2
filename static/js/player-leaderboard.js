class PlayerLeaderboard {
    constructor() {
        this.API_URL = "";  // Use relative URLs since we're on the same server
        this.fullLeaderboardData = [];
        this.totalPlayersOnBoard = 0;
        this.init();
    }

    init() {
        this.bindElements();
        this.setupEventListeners();
        this.initializePage();
    }

    bindElements() {
        this.leaderboardSelector = document.getElementById('leaderboardSelector');
        this.gameModeSelector = document.querySelector('.game-mode-selector');
        this.leaderboardBody = document.getElementById('leaderboardBody');
        this.leaderboardTitle = document.getElementById('leaderboardTitle');
        this.searchInput = document.getElementById('searchInput');
        this.searchResultsContainer = document.getElementById('searchResults');
        this.statusHeader = document.getElementById('status-header');
    }

    setupEventListeners() {
        this.leaderboardSelector.addEventListener('change', () => this.updateLeaderboard());
        
        this.gameModeSelector.addEventListener('click', (e) => {
            if (e.target.tagName === 'BUTTON') {
                this.gameModeSelector.querySelector('button.active').classList.remove('active');
                e.target.classList.add('active');
                this.updateLeaderboard();
            }
        });

        this.searchInput.addEventListener('input', Utils.debounce(() => this.handleAutocompleteSearch(), 300));
    }

    async updateLeaderboard() {
        const groupCode = this.leaderboardSelector.value;
        const gameMode = this.gameModeSelector.querySelector('button.active').dataset.mode;

        const selectedOption = this.leaderboardSelector.options[this.leaderboardSelector.selectedIndex];
        const groupName = selectedOption ? selectedOption.text : 'Leaderboard';

        if (!groupCode || !gameMode) return;

        this.leaderboardTitle.textContent = `Loading...`;
        Utils.showMessage(this.leaderboardBody, 'Loading...');
        this.searchInput.value = '';
        this.searchResultsContainer.innerHTML = '';

        const endpoint = (groupCode === 'global')
            ? `${this.API_URL}/api/leaderboard/global/${gameMode}`
            : `${this.API_URL}/api/leaderboard/${groupCode}/${gameMode}`;

        try {
            const response = await fetch(endpoint);
            const data = await response.json();
            this.fullLeaderboardData = data.players;
            this.totalPlayersOnBoard = data.total_players;

            const topN = Math.min(50, this.totalPlayersOnBoard);
            this.leaderboardTitle.textContent = `Top ${topN} of ${this.totalPlayersOnBoard} Players - ${groupName} - ${gameMode}`;

            this.displayLeaderboard(this.fullLeaderboardData.slice(0, 50));
        } catch (error) {
            console.error("Failed to fetch leaderboard:", error);
            this.leaderboardTitle.textContent = "Error";
            Utils.showMessage(this.leaderboardBody, 'Error loading data.', true);
        }
    }

    displayLeaderboard(playerList) {
        this.leaderboardBody.innerHTML = "";
        if (!playerList || playerList.length === 0) {
            Utils.showMessage(this.leaderboardBody, 'No ranked players found.');
            return;
        }
        
        // Use document fragment for better performance with large lists
        const fragment = document.createDocumentFragment();
        
        playerList.forEach(player => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td class="rank">${player.rank}</td>
                <td>${player.display_name}</td>
                <td>${player.leaderboard_rating.toFixed(2)}</td>
            `;
            fragment.appendChild(row);
        });
        
        this.leaderboardBody.appendChild(fragment);
    }

    handleAutocompleteSearch() {
        const searchTerm = this.searchInput.value.trim().toLowerCase();
        if (!searchTerm) {
            this.searchResultsContainer.innerHTML = '';
            return;
        }
        
        const results = this.fullLeaderboardData.filter(p => 
            p.name.toLowerCase().includes(searchTerm)
        );

        let searchHTML = `<h2>Search Results</h2>`;
        if (results.length === 0) {
            searchHTML += `<p class="message">No players found matching "${this.searchInput.value}".</p>`;
        } else {
            searchHTML += `
                <table>
                    <thead><tr>
                        <th style="width:10%; text-align: center;">Rank</th>
                        <th style="width:60%;">Player Name</th>
                        <th style="width:30%;">Leaderboard Rating</th>
                    </tr></thead>
                    <tbody>
                        ${results.map(player => `
                            <tr>
                                <td class="rank">${player.rank}</td>
                                <td>${player.display_name}</td>
                                <td>${player.leaderboard_rating.toFixed(2)}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>`;
        }
        this.searchResultsContainer.innerHTML = searchHTML;
    }

    async fetchStatus() {
        try {
            const response = await fetch(`${this.API_URL}/api/status`);
            const data = await response.json();
            this.statusHeader.textContent = `Source data last updated: ${data.last_updated}`;
        } catch (error) {
            console.error("Failed to fetch status:", error);
            this.statusHeader.textContent = 'Could not retrieve update status.';
        }
    }

    async initializePage() {
        this.fetchStatus();
        try {
            const response = await fetch(`${this.API_URL}/api/leaderboards`);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);

            const data = await response.json();
            this.leaderboardSelector.innerHTML = "";

            const createOptgroup = (label, items) => {
                const group = document.createElement('optgroup');
                group.label = label;
                items.forEach(item => {
                    const option = document.createElement('option');
                    option.value = item.code;
                    option.textContent = item.name;
                    group.appendChild(option);
                });
                return group;
            };

            const globalOption = document.createElement('option');
            globalOption.value = 'global';
            globalOption.textContent = 'Global World Ranking';
            this.leaderboardSelector.appendChild(globalOption);

            if (data.nations && data.nations.length > 0) {
                this.leaderboardSelector.appendChild(createOptgroup('Nations', data.nations));
            }
            if (data.regions && data.regions.length > 0) {
                this.leaderboardSelector.appendChild(createOptgroup('Regions', data.regions));
            }

            this.leaderboardSelector.value = 'global';
            await this.updateLeaderboard();
        } catch (error) {
            console.error("Failed to fetch leaderboard list:", error);
            this.leaderboardSelector.innerHTML = `<option>Error</option>`;
            Utils.showMessage(this.leaderboardBody, 'Could not connect to backend.', true);
        }
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.playerLeaderboard = new PlayerLeaderboard();
});
