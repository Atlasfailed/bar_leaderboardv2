// Energy Efficiency Calculator JavaScript

console.log('=== EFFICIENCY ANALYSIS SCRIPT LOADED ===');

// Store efficiency data for calculations
let efficiencyData = null;

// Unit display names mapping
const unitDisplayNames = {
    'armsol': 'Solar Generator',
    'armwin': 'Wind Generator',
    'armtide': 'Tidal Generator', 
    'armfus': 'Fusion Reactor',
    'armgeo': 'Geothermal Generator',
    'armafus': 'Advanced Fusion Reactor',
    'armadvsol': 'Advanced Solar Generator',
    'armageo': 'Advanced Geothermal Powerplant',
    'armckfus': 'Cloakable Fusion Reactor',
    'armuwfus': 'Underwater Fusion Reactor',
    'armgmm': 'Prude (Safe Geothermal)',
    'corsol': 'Solar Generator',
    'corwin': 'Wind Generator',
    'cortide': 'Tidal Generator',
    'corfus': 'Fusion Reactor',
    'corgeo': 'Geothermal Generator'
};

document.addEventListener('DOMContentLoaded', function() {
    console.log('=== DOM CONTENT LOADED EVENT FIRED ===');
    
    // Add a visible indicator that JS is working
    const tableBody = document.getElementById('efficiency-table-body');
    if (tableBody) {
        console.log('Table body found immediately in DOMContentLoaded');
        tableBody.innerHTML = '<tr class="loading-row"><td colspan="7" class="loading-cell"><div class="loading-indicator"><div class="spinner"></div><span>JavaScript is working, initializing...</span></div></td></tr>';
    } else {
        console.error('Table body NOT found in DOMContentLoaded!');
        alert('ERROR: Table body element not found!');
    }
    
    // Initialize controls
    initializeControls();
    
    // Initialize modal
    initializeModal();
    
    // Load efficiency data
    fetchEfficiencyData();
});

function initializeControls() {
    // Faction selector
    const factionSelect = document.getElementById('faction-select');
    factionSelect.addEventListener('change', updateResults);
    
    // Wind speed slider
    const windSlider = document.getElementById('wind-slider');
    const windValue = document.getElementById('wind-value');
    windSlider.addEventListener('input', function() {
        windValue.textContent = this.value;
        updateResults();
    });
    
    // Tidal speed slider
    const tidalSlider = document.getElementById('tidal-slider');
    const tidalValue = document.getElementById('tidal-value');
    tidalSlider.addEventListener('input', function() {
        tidalValue.textContent = this.value;
        updateResults();
    });
    
    // Efficiency type selector
    const efficiencyType = document.getElementById('efficiency-type');
    efficiencyType.addEventListener('change', updateResults);
    
    // Don't call updateResults() here - data isn't loaded yet
}

async function fetchEfficiencyData() {
    console.log('Fetching efficiency data...');
    try {
        const response = await fetch('/api/efficiency-data');
        console.log('Response received:', response.status);
        const data = await response.json();
        console.log('Data parsed:', data);
        
        if (data.error) {
            console.error('Error fetching efficiency data:', data.error);
            showError('Failed to load efficiency data');
            return;
        }
        
        console.log('Setting efficiencyData and calling updateResults...');
        efficiencyData = data;
        updateResults();
        
    } catch (error) {
        console.error('Error fetching efficiency data:', error);
        showError('Failed to load efficiency data');
    }
}

function updateResults() {
    console.log('updateResults called, efficiencyData:', efficiencyData);
    if (!efficiencyData) {
        console.log('No efficiency data, showing loading...');
        showLoading();
        return;
    }
    
    const faction = document.getElementById('faction-select').value;
    const windSpeed = parseInt(document.getElementById('wind-slider').value);
    const tidalSpeed = parseInt(document.getElementById('tidal-slider').value);
    const efficiencyType = document.getElementById('efficiency-type').value;
    
    console.log('Parameters:', {faction, windSpeed, tidalSpeed, efficiencyType});
    
    // Get faction data
    const factionData = faction === 'ARM' ? efficiencyData.arm_data : efficiencyData.cor_data;
    console.log('Faction data length:', factionData?.length);
    
    // Calculate efficiency for each unit
    const results = calculateEfficiencies(factionData, windSpeed, tidalSpeed, efficiencyType);
    
    // Update results table
    updateTable(results, faction, windSpeed, tidalSpeed, efficiencyType);
}

function calculateEfficiencies(factionData, windSpeed, tidalSpeed, efficiencyType) {
    console.log('calculateEfficiencies called with:', {factionData: factionData?.length, windSpeed, tidalSpeed, efficiencyType});
    const results = [];
    
    // Filter to get unique units and find the right speed data
    const uniqueUnits = [];
    const seenUnits = new Set();
    
    factionData.forEach(unit => {
        if (!seenUnits.has(unit.unit_name)) {
            seenUnits.add(unit.unit_name);
            
            // For variable energy units, find the data for the current speed
            if (unit.is_variable_energy) {
                let targetSpeed = 15; // default
                if (unit.unit_name.includes('win')) {
                    targetSpeed = windSpeed;
                } else if (unit.unit_name.includes('tide')) {
                    targetSpeed = tidalSpeed;
                }
                
                // Find the unit data for the target speed
                const speedSpecificUnit = factionData.find(u => 
                    u.unit_name === unit.unit_name && u.wind_tidal_speed === targetSpeed
                );
                
                if (speedSpecificUnit) {
                    uniqueUnits.push(speedSpecificUnit);
                } else {
                    uniqueUnits.push(unit); // fallback
                }
            } else {
                uniqueUnits.push(unit);
            }
        }
    });
    
    console.log('Unique units found:', uniqueUnits.length);
    
    uniqueUnits.forEach(unit => {
        const efficiency = calculateUnitEfficiency(unit, efficiencyType);
        
        results.push({
            unit_name: unit.unit_name,
            display_name: unitDisplayNames[unit.unit_name] || unit.display_name || unit.unit_name,
            efficiency: efficiency,
            energy_output: unit.energy_output, // Use the API's calculated energy output
            metal_cost: unit.metalcost,
            energy_cost: unit.energycost,
            build_time: unit.buildtime,
            is_variable: unit.is_variable_energy
        });
    });
    
    console.log('Results before sorting:', results.length);
    
    // Sort by efficiency (highest first)
    results.sort((a, b) => b.efficiency - a.efficiency);
    
    // Find wind generator efficiency for baseline comparison
    const windUnit = results.find(r => r.unit_name.includes('win'));
    const windEfficiency = windUnit ? windUnit.efficiency : results[0].efficiency;
    
    // Add comparison percentages
    results.forEach(result => {
        if (result.unit_name.includes('win')) {
            result.comparison = 0; // Baseline
            result.comparisonText = 'Baseline (100%)';
        } else {
            const percentage = ((result.efficiency / windEfficiency) - 1) * 100;
            result.comparison = percentage;
            if (percentage > 0) {
                result.comparisonText = `+${percentage.toFixed(1)}%`;
            } else {
                result.comparisonText = `${percentage.toFixed(1)}%`;
            }
        }
    });
    
    console.log('Final results:', results.length, results.slice(0, 3)); // Log first 3 results
    return results;
}

function calculateUnitEfficiency(unit, efficiencyType) {
    const energyOutput = unit.energy_output; // API already has correct energy output
    
    if (efficiencyType === 'metal') {
        // Metal efficiency: Energy per 100 metal (including energy cost converted at 70:1)
        const totalMetalCost = unit.metalcost + (unit.energycost / 70);
        return (energyOutput / totalMetalCost) * 100;
    } else {
        // Time efficiency: Energy per 1000 build time
        return (energyOutput / unit.buildtime) * 1000;
    }
}

function updateTable(results, faction, windSpeed, tidalSpeed, efficiencyType) {
    console.log('updateTable called with results length:', results?.length, 'faction:', faction);
    
    const tableBody = document.getElementById('efficiency-table-body');
    console.log('Table body element found:', !!tableBody);
    
    if (!tableBody) {
        console.error('efficiency-table-body element not found in DOM!');
        return;
    }
    const resultsTitle = document.getElementById('results-title');
    
    // Update title
    const efficiencyTypeText = efficiencyType === 'metal' ? 'Metal' : 'Time';
    resultsTitle.textContent = `${faction} ${efficiencyTypeText} Efficiency Ranking (Wind: ${windSpeed}, Tidal: ${tidalSpeed})`;
    
    // Find wind efficiency for modal details
    const windUnit = results.find(r => r.unit_name.includes('win'));
    const windEfficiency = windUnit ? windUnit.efficiency : results[0].efficiency;
    
    // Clear existing rows
    tableBody.innerHTML = '';
    
    console.log('About to create rows for', results.length, 'results');
    
    results.forEach((result, index) => {
        console.log(`Creating row ${index} for ${result.display_name}`);
        const row = document.createElement('tr');
        row.className = 'efficiency-row';
        row.style.cursor = 'pointer';
        row.title = 'Click to see calculation details';
        
        // Add click handler to the entire row
        row.addEventListener('click', function() {
            showCalculationDetails(result, efficiencyType, windEfficiency);
        });
        
        // Rank
        const rankCell = document.createElement('td');
        rankCell.className = 'rank-cell';
        const rankBadge = document.createElement('span');
        rankBadge.className = `rank-badge rank-${index < 3 ? index + 1 : 'other'}`;
        rankBadge.textContent = index + 1;
        rankCell.appendChild(rankBadge);
        row.appendChild(rankCell);
        
        // Generator name
        const nameCell = document.createElement('td');
        nameCell.className = 'generator-cell';
        nameCell.textContent = result.display_name;
        row.appendChild(nameCell);
        
        // Efficiency index
        const efficiencyCell = document.createElement('td');
        efficiencyCell.className = 'efficiency-index';
        efficiencyCell.textContent = result.efficiency.toFixed(2);
        row.appendChild(efficiencyCell);
        
        // Comparison to wind
        const comparisonCell = document.createElement('td');
        comparisonCell.className = 'comparison-cell';
        if (result.comparison > 0) {
            comparisonCell.classList.add('comparison-positive');
        } else if (result.comparison < 0) {
            comparisonCell.classList.add('comparison-negative');
        } else {
            comparisonCell.classList.add('comparison-baseline');
        }
        comparisonCell.textContent = result.comparisonText;
        row.appendChild(comparisonCell);
        
        // Energy output
        const energyCell = document.createElement('td');
        energyCell.className = 'energy-output';
        energyCell.textContent = result.energy_output.toFixed(1);
        row.appendChild(energyCell);
        
        // Cost information
        const costCell = document.createElement('td');
        costCell.className = 'cost-cell';
        let costText = `${result.metal_cost}M`;
        if (result.energy_cost > 0) {
            costText += ` + ${result.energy_cost}E`;
        }
        costCell.innerHTML = `${costText}<br><small>${result.build_time}s</small>`;
        row.appendChild(costCell);
        
        // Details indicator
        const detailsCell = document.createElement('td');
        detailsCell.className = 'details-cell';
        const detailsBtn = document.createElement('button');
        detailsBtn.className = 'details-btn';
        detailsBtn.textContent = 'View Details';
        detailsBtn.onclick = () => showCalculationDetails(result, efficiencyType, windEfficiency);
        detailsCell.appendChild(detailsBtn);
        row.appendChild(detailsCell);
        
        tableBody.appendChild(row);
    });
    
    console.log('Finished adding', results.length, 'rows to table. Table body now has', tableBody.children.length, 'rows');
}

function showLoading() {
    const tableBody = document.getElementById('efficiency-table-body');
    console.log('showLoading called, tableBody element:', tableBody);
    if (tableBody) {
        tableBody.innerHTML = '<tr><td colspan="7" class="efficiency-table-loading">Loading efficiency data...</td></tr>';
    } else {
        console.error('Table body element not found!');
    }
}

function showError(message) {
    const tableBody = document.getElementById('efficiency-table-body');
    tableBody.innerHTML = `<tr><td colspan="7" class="efficiency-table-loading" style="color: var(--negative-score);">❌ ${message}</td></tr>`;
}

// Modal functionality
function initializeModal() {
    const modal = document.getElementById('calculation-modal');
    const closeBtn = document.querySelector('.modal-close');
    
    // Close modal when clicking the X
    closeBtn.addEventListener('click', function() {
        modal.style.display = 'none';
    });
    
    // Close modal when clicking outside of it
    window.addEventListener('click', function(event) {
        if (event.target === modal) {
            modal.style.display = 'none';
        }
    });
    
    // Close modal on Escape key
    document.addEventListener('keydown', function(event) {
        if (event.key === 'Escape' && modal.style.display === 'flex') {
            modal.style.display = 'none';
        }
    });
}

function showCalculationDetails(result, efficiencyType, windEfficiency) {
    const modal = document.getElementById('calculation-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalContent = document.getElementById('modal-content');
    
    modalTitle.textContent = `${result.display_name} - Calculation Details`;
    
    // Build the calculation breakdown
    let calculationHTML = '<div class="calculation-details">';
    
    // Basic unit info
    calculationHTML += `
        <div class="calculation-section">
            <h4>Unit Information</h4>
            <div class="calculation-row">
                <span class="calculation-label">Unit Name:</span>
                <span class="calculation-value">${result.display_name}</span>
            </div>
            <div class="calculation-row">
                <span class="calculation-label">Energy Output:</span>
                <span class="calculation-value">${result.energy_output.toFixed(1)} energy/sec</span>
            </div>
            <div class="calculation-row">
                <span class="calculation-label">Metal Cost:</span>
                <span class="calculation-value">${result.metal_cost} metal</span>
            </div>
            <div class="calculation-row">
                <span class="calculation-label">Energy Cost:</span>
                <span class="calculation-value">${result.energy_cost} energy</span>
            </div>
            <div class="calculation-row">
                <span class="calculation-label">Build Time:</span>
                <span class="calculation-value">${result.build_time} seconds</span>
            </div>
        </div>
    `;
    
    // Efficiency calculation breakdown
    if (efficiencyType === 'metal') {
        const energyCostInMetal = result.energy_cost / 70;
        const totalMetalCost = result.metal_cost + energyCostInMetal;
        
        calculationHTML += `
            <div class="calculation-section">
                <h4>Metal Efficiency Calculation</h4>
                <div class="calculation-row">
                    <span class="calculation-label">Energy Cost ÷ 70:</span>
                    <span class="calculation-value">${result.energy_cost} ÷ 70 = ${energyCostInMetal.toFixed(2)} metal</span>
                </div>
                <div class="calculation-row">
                    <span class="calculation-label">Total Metal Cost:</span>
                    <span class="calculation-value">${result.metal_cost} + ${energyCostInMetal.toFixed(2)} = ${totalMetalCost.toFixed(2)} metal</span>
                </div>
                <div class="calculation-row">
                    <span class="calculation-label">Formula:</span>
                    <span class="calculation-value">(${result.energy_output.toFixed(1)} ÷ ${totalMetalCost.toFixed(2)}) × 100</span>
                </div>
                <div class="calculation-row">
                    <span class="calculation-label calculation-final">Efficiency Index:</span>
                    <span class="calculation-value calculation-final">${result.efficiency.toFixed(2)}</span>
                </div>
            </div>
        `;
    } else {
        calculationHTML += `
            <div class="calculation-section">
                <h4>Time Efficiency Calculation</h4>
                <div class="calculation-row">
                    <span class="calculation-label">Formula:</span>
                    <span class="calculation-value">(${result.energy_output.toFixed(1)} ÷ ${result.build_time}) × 1000</span>
                </div>
                <div class="calculation-row">
                    <span class="calculation-label calculation-final">Efficiency Index:</span>
                    <span class="calculation-value calculation-final">${result.efficiency.toFixed(2)}</span>
                </div>
            </div>
        `;
    }
    
    // Comparison to wind
    if (result.unit_name.includes('win')) {
        calculationHTML += `
            <div class="comparison-section">
                <h4>Baseline Reference</h4>
                <p>This is the <span class="comparison-baseline">Wind Generator</span> - used as the baseline (100%) for all comparisons.</p>
            </div>
        `;
    } else {
        const percentage = ((result.efficiency / windEfficiency) - 1) * 100;
        const comparisonClass = percentage > 0 ? 'comparison-positive' : 'comparison-negative';
        const comparisonSymbol = percentage > 0 ? '+' : '';
        
        calculationHTML += `
            <div class="comparison-section">
                <h4>Comparison to Wind Generator</h4>
                <div class="calculation-row">
                    <span class="calculation-label">Wind Generator Efficiency:</span>
                    <span class="calculation-value">${windEfficiency.toFixed(2)}</span>
                </div>
                <div class="calculation-row">
                    <span class="calculation-label">Calculation:</span>
                    <span class="calculation-value">((${result.efficiency.toFixed(2)} ÷ ${windEfficiency.toFixed(2)}) - 1) × 100%</span>
                </div>
                <div class="calculation-row">
                    <span class="calculation-label">Result:</span>
                    <span class="calculation-value ${comparisonClass}">${comparisonSymbol}${percentage.toFixed(1)}%</span>
                </div>
                <p style="margin-top: 1rem; color: var(--text-secondary); font-size: 0.9rem;">
                    ${percentage > 0 ? 'This generator is MORE efficient than wind.' : 'This generator is LESS efficient than wind.'}
                </p>
            </div>
        `;
    }
    
    calculationHTML += '</div>';
    modalContent.innerHTML = calculationHTML;
    modal.style.display = 'flex';
}
