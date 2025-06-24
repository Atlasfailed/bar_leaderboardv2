// Efficiency Analysis JavaScript
let allUnits = {}; // Store unified unit data for shared legend
let colorMapping = {}; // Store consistent color mapping

document.addEventListener('DOMContentLoaded', function() {
    // Load efficiency data and create plots
    fetchEfficiencyData();
});

async function fetchEfficiencyData() {
    try {
        const response = await fetch('/api/efficiency-data');
        const data = await response.json();
        
        if (data.error) {
            console.error('Error fetching efficiency data:', data.error);
            return;
        }
        
        // Process unit data for unified legend
        processUnitsForLegend(data.arm_data);
        processUnitsForLegend(data.cor_data);
        
        // Create color mapping for consistency
        createColorMapping();
        
        // Create shared legend first
        createUnifiedLegend();
        
        // Create all four plots with intuitive units
        createEfficiencyPlot(data.arm_data, 'metal_efficiency_per_100', 'arm-metal-efficiency', 'ARM', 'Energy per 100 Metal');
        createEfficiencyPlot(data.arm_data, 'time_efficiency_per_1000', 'arm-time-efficiency', 'ARM', 'Energy per 1000 Build Time');
        createEfficiencyPlot(data.cor_data, 'metal_efficiency_per_100', 'cor-metal-efficiency', 'COR', 'Energy per 100 Metal');
        createEfficiencyPlot(data.cor_data, 'time_efficiency_per_1000', 'cor-time-efficiency', 'COR', 'Energy per 1000 Build Time');
        
    } catch (error) {
        console.error('Error fetching efficiency data:', error);
    }
}

function processUnitsForLegend(data) {
    data.forEach(row => {
        const unitName = row.display_name || row.unit_name;
        if (!allUnits[unitName]) {
            allUnits[unitName] = {
                unit_name: unitName,
                description: row.description,
                is_variable: row.is_variable_energy
            };
        }
    });
}

function createColorMapping() {
    const colors = ['#58a6ff', '#28a745', '#dc3545', '#ffc107', '#17a2b8', '#6f42c1', '#fd7e14', '#20c997', '#e83e8c', '#6610f2'];
    
    // Sort units consistently: variable energy first, then fixed
    const sortedUnits = Object.values(allUnits).sort((a, b) => {
        if (a.is_variable && !b.is_variable) return -1;
        if (!a.is_variable && b.is_variable) return 1;
        return a.unit_name.localeCompare(b.unit_name);
    });
    
    // Assign colors consistently
    sortedUnits.forEach((unit, index) => {
        colorMapping[unit.unit_name] = colors[index % colors.length];
    });
}

function createUnifiedLegend() {
    const container = document.getElementById('unified-legend-items');
    if (!container) {
        console.error('Legend container not found');
        return;
    }
    
    // Clear existing content
    container.innerHTML = '';
    
    // Sort units: variable energy first, then fixed
    const sortedUnits = Object.values(allUnits).sort((a, b) => {
        if (a.is_variable && !b.is_variable) return -1;
        if (!a.is_variable && b.is_variable) return 1;
        return a.unit_name.localeCompare(b.unit_name);
    });
    
    const legendItemsDiv = document.createElement('div');
    legendItemsDiv.className = 'legend-items';
    
    sortedUnits.forEach(unit => {
        const color = colorMapping[unit.unit_name];
        const isVariable = unit.is_variable;
        
        const legendItem = document.createElement('div');
        legendItem.className = 'legend-item';
        
        const colorDiv = document.createElement('div');
        colorDiv.className = `legend-color ${isVariable ? '' : 'dashed'}`;
        
        if (isVariable) {
            colorDiv.style.backgroundColor = color;
        } else {
            colorDiv.style.borderColor = color;
            colorDiv.style.backgroundColor = 'transparent';
        }
        
        const textDiv = document.createElement('div');
        textDiv.className = 'legend-text';
        textDiv.textContent = unit.unit_name;
        
        legendItem.appendChild(colorDiv);
        legendItem.appendChild(textDiv);
        legendItemsDiv.appendChild(legendItem);
    });
    
    container.appendChild(legendItemsDiv);
}

function createEfficiencyPlot(data, efficiencyType, containerId, faction, efficiencyLabel) {
    // Group data by unit name for different traces
    const unitGroups = {};
    
    data.forEach(row => {
        const unitName = row.display_name || row.unit_name;
        if (!unitGroups[unitName]) {
            unitGroups[unitName] = {
                x: [],
                y: [],
                unit_name: unitName,
                description: row.description,
                is_variable: row.is_variable_energy
            };
        }
        unitGroups[unitName].x.push(row.wind_tidal_speed);
        unitGroups[unitName].y.push(row[efficiencyType]);
    });
    
    // Create traces for each unit
    const traces = [];
    
    // Sort units: variable energy first (more interesting), then fixed
    const sortedUnits = Object.values(unitGroups).sort((a, b) => {
        if (a.is_variable && !b.is_variable) return -1;
        if (!a.is_variable && b.is_variable) return 1;
        return a.unit_name.localeCompare(b.unit_name);
    });
    
    sortedUnits.forEach(unit => {
        const isVariable = unit.is_variable;
        const color = colorMapping[unit.unit_name];
        
        traces.push({
            x: unit.x,
            y: unit.y,
            mode: isVariable ? 'lines+markers' : 'lines',
            name: unit.unit_name,
            showlegend: false, // Hide individual legends
            line: {
                color: color,
                width: isVariable ? 3 : 2,
                dash: isVariable ? 'solid' : 'dash'
            },
            marker: {
                size: isVariable ? 6 : 4,
                color: color
            },
            hovertemplate: 
                '<b>%{fullData.name}</b><br>' +
                'Wind/Tidal Speed: %{x}<br>' +
                efficiencyLabel + ': %{y:.1f}<br>' +
                '<extra></extra>'
        });
    });
    
    const layout = {
        xaxis: {
            title: {
                text: 'Wind/Tidal Speed',
                font: { color: '#c9d1d9' }
            },
            color: '#c9d1d9',
            gridcolor: '#30363d',
            tickfont: { color: '#8b949e' },
            range: [1, 20]
        },
        yaxis: {
            title: {
                text: efficiencyLabel,
                font: { color: '#c9d1d9' }
            },
            color: '#c9d1d9',
            gridcolor: '#30363d',
            tickfont: { color: '#8b949e' }
        },
        plot_bgcolor: '#161b22',
        paper_bgcolor: '#161b22',
        font: { color: '#c9d1d9' },
        showlegend: false, // Disable legend entirely
        hovermode: 'closest', // Only show tooltip for closest point
        margin: {
            l: 60,
            r: 30,
            t: 20,
            b: 50
        }
    };
    
    const config = {
        responsive: true,
        displayModeBar: true,
        modeBarButtonsToRemove: ['pan2d', 'lasso2d', 'select2d'],
        displaylogo: false,
        toImageButtonOptions: {
            format: 'png',
            filename: `${faction}_${efficiencyType}_analysis`,
            height: 500,
            width: 700,
            scale: 1
        }
    };
    
    Plotly.newPlot(containerId, traces, layout, config);
}

// Utility function to format numbers
function formatNumber(num) {
    if (num >= 1000000) {
        return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
        return (num / 1000).toFixed(1) + 'K';
    } else {
        return num.toFixed(2);
    }
}
