// ===========================
// Global Variables & State
// ===========================
let currentPage = 'home';
let uploadedData = null;
let segmentationResults = null;
let charts = {};

// Sample data for demonstration
const sampleClusters = [120, 80, 50, 40];
const sampleScatterData = [
    { x: 50, y: 1500, segment: 0 },
    { x: 45, y: 1800, segment: 0 },
    { x: 52, y: 1400, segment: 0 },
    { x: 35, y: 800, segment: 1 },
    { x: 38, y: 750, segment: 1 },
    { x: 40, y: 900, segment: 1 },
    { x: 28, y: 600, segment: 2 },
    { x: 25, y: 550, segment: 2 },
    { x: 30, y: 650, segment: 2 },
    { x: 60, y: 2000, segment: 3 },
    { x: 65, y: 2200, segment: 3 }
];

// ===========================
// Page Navigation
// ===========================
function navigateTo(pageName) {
    // Hide all pages
    document.querySelectorAll('.page').forEach(page => {
        page.classList.remove('active');
    });

    // Remove active class from nav links
    document.querySelectorAll('.nav-link').forEach(link => {
        link.classList.remove('active');
    });

    // Show requested page
    const targetPage = document.getElementById(pageName);
    if (targetPage) {
        targetPage.classList.add('active');
        currentPage = pageName;

        // Add active class to corresponding nav link
        const navLink = document.querySelector(`[data-page="${pageName}"]`);
        if (navLink) {
            navLink.classList.add('active');
        }

        // Initialize charts if on visualization page
        if (pageName === 'visualization') {
            setTimeout(initializeCharts, 100);
        }
    }
}

// ===========================
// Navigation Links Event Listeners
// ===========================
document.querySelectorAll('[data-page]').forEach(link => {
    link.addEventListener('click', (e) => {
        e.preventDefault();
        const page = e.target.dataset.page;
        navigateTo(page);
    });
});

// ===========================
// HOME PAGE Functions
// ===========================
function openSparkUI() {
    window.open('http://localhost:4040', '_blank');
}

// ===========================
// UPLOAD PAGE Functions
// ===========================
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');

if (dropZone) {
    dropZone.addEventListener('click', () => fileInput.click());

    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('dragover');
    });

    dropZone.addEventListener('dragleave', () => {
        dropZone.classList.remove('dragover');
    });

    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.classList.remove('dragover');
        handleFileSelect(e.dataTransfer.files);
    });

    fileInput.addEventListener('change', (e) => {
        handleFileSelect(e.target.files);
    });
}

function handleFileSelect(files) {
    if (files.length === 0) return;

    const file = files[0];

    // Validate file type
    if (!file.type === 'text/csv' && !file.name.endsWith('.csv')) {
        showMessage('❌ Please upload a CSV file', 'error');
        return;
    }

    // Read file
    const reader = new FileReader();
    reader.onload = (e) => {
        try {
            const csvData = e.target.result;
            uploadedData = parseCSV(csvData);
            displayPreview(uploadedData);
            document.getElementById('segmentationBtn').disabled = false;
            addLog('[INFO] File uploaded successfully. ' + uploadedData.rows.length + ' rows detected.');
            showMessage('✅ File uploaded successfully!');
        } catch (error) {
            showMessage('❌ Error parsing CSV: ' + error.message, 'error');
            addLog('[ERROR] ' + error.message);
        }
    };
    reader.readAsText(file);
}

function parseCSV(csvData) {
    const lines = csvData.trim().split('\n');
    const headers = lines[0].split(',').map(h => h.trim());
    const rows = [];

    for (let i = 1; i < lines.length && i <= 10; i++) {
        const values = lines[i].split(',').map(v => v.trim());
        const row = {};
        headers.forEach((header, index) => {
            row[header] = values[index] || '';
        });
        rows.push(row);
    }

    return { headers, rows };
}

function displayPreview(data) {
    const previewContainer = document.getElementById('previewContainer');
    let html = '<table><thead><tr>';

    // Add headers
    data.headers.forEach(header => {
        html += `<th>${header}</th>`;
    });
    html += '</tr></thead><tbody>';

    // Add rows
    data.rows.forEach(row => {
        html += '<tr>';
        data.headers.forEach(header => {
            html += `<td>${row[header]}</td>`;
        });
        html += '</tr>';
    });

    html += '</tbody></table>';
    previewContainer.innerHTML = html;
}

function clearUpload() {
    uploadedData = null;
    fileInput.value = '';
    document.getElementById('previewContainer').innerHTML = '<p class="empty-state">No file selected yet. Upload a CSV to see a preview.</p>';
    document.getElementById('segmentationBtn').disabled = true;
    showMessage('Upload cleared');
}

function startSegmentation() {
    if (!uploadedData) {
        showMessage('❌ Please upload data first', 'error');
        return;
    }

    addLog('[INFO] Starting segmentation process...');
    addLog('[INFO] Sending data to Spark backend...');

    // Simulate segmentation progress
    let progress = 0;
    const progressBar = document.getElementById('progressBar');
    const progressText = document.getElementById('progressText');

    const stages = document.querySelectorAll('.stage-status');

    // Simulate stages
    setTimeout(() => {
        stages[0].textContent = 'Completed';
        stages[0].className = 'stage-status status-completed';
        addLog('[INFO] Stage 1: Data Loading - Completed');
    }, 1000);

    setTimeout(() => {
        stages[1].textContent = 'Running';
        stages[1].className = 'stage-status status-running';
        addLog('[INFO] Stage 2: Data Preprocessing - Running');
    }, 2000);

    setTimeout(() => {
        stages[1].textContent = 'Completed';
        stages[1].className = 'stage-status status-completed';
        stages[2].textContent = 'Running';
        stages[2].className = 'stage-status status-running';
        addLog('[INFO] Stage 2: Data Preprocessing - Completed');
        addLog('[INFO] Stage 3: Feature Engineering - Running');
    }, 4000);

    setTimeout(() => {
        stages[2].textContent = 'Completed';
        stages[2].className = 'stage-status status-completed';
        stages[3].textContent = 'Running';
        stages[3].className = 'stage-status status-running';
        addLog('[INFO] Stage 3: Feature Engineering - Completed');
        addLog('[INFO] Stage 4: Clustering (K-Means) - Running');
    }, 6000);

    setTimeout(() => {
        stages[3].textContent = 'Completed';
        stages[3].className = 'stage-status status-completed';
        stages[4].textContent = 'Running';
        stages[4].className = 'stage-status status-running';
        addLog('[INFO] Stage 4: Clustering (K-Means) - Completed');
        addLog('[INFO] Stage 5: Results Generation - Running');
    }, 8000);

    setTimeout(() => {
        stages[4].textContent = 'Completed';
        stages[4].className = 'stage-status status-completed';
        addLog('[INFO] Stage 5: Results Generation - Completed');
        addLog('[SUCCESS] Segmentation completed successfully!');
        
        // Generate mock results
        generateMockResults();
        showMessage('✅ Segmentation completed! View results on the Segmentation Results page.');
        
        // Update progress
        progress = 100;
        progressBar.style.width = progress + '%';
        progressText.textContent = progress + '% Complete';
    }, 10000);

    // Animate progress bar
    const progressInterval = setInterval(() => {
        if (progress < 95) {
            progress += Math.random() * 15;
            if (progress > 95) progress = 95;
            progressBar.style.width = progress + '%';
            progressText.textContent = Math.round(progress) + '% Complete';
        } else {
            clearInterval(progressInterval);
        }
    }, 500);
}

// ===========================
// JOB MONITOR Functions
// ===========================
function addLog(message) {
    const logConsole = document.getElementById('logConsole');
    const timestamp = new Date().toLocaleTimeString();
    const logEntry = document.createElement('p');
    logEntry.className = 'log-entry';
    
    if (message.includes('[ERROR]')) {
        logEntry.className = 'log-entry error';
    } else if (message.includes('[WARNING]')) {
        logEntry.className = 'log-entry warning';
    } else if (message.includes('[SUCCESS]')) {
        logEntry.className = 'log-entry';
        logEntry.style.color = '#10b981';
    }

    logEntry.textContent = `[${timestamp}] ${message}`;
    logConsole.appendChild(logEntry);
    logConsole.scrollTop = logConsole.scrollHeight;
}

function clearLogs() {
    const logConsole = document.getElementById('logConsole');
    logConsole.innerHTML = '<p class="log-entry">[INFO] Logs cleared</p>';
    showMessage('Logs cleared');
}

// ===========================
// SEGMENTATION RESULTS Functions
// ===========================
function generateMockResults() {
    segmentationResults = {
        numClusters: 4,
        totalCustomers: 290,
        clusters: [
            {
                id: 0,
                size: 120,
                avgAge: 45,
                avgSpend: 1550,
                topCategory: 'Electronics',
                description: 'Premium Customers'
            },
            {
                id: 1,
                size: 80,
                avgAge: 38,
                avgSpend: 820,
                topCategory: 'Fashion',
                description: 'Regular Shoppers'
            },
            {
                id: 2,
                size: 50,
                avgAge: 28,
                avgSpend: 600,
                topCategory: 'Home & Garden',
                description: 'Budget Conscious'
            },
            {
                id: 3,
                size: 40,
                avgAge: 62,
                avgSpend: 2100,
                topCategory: 'Home & Garden',
                description: 'Luxury Segment'
            }
        ],
        silhouetteScore: 0.72
    };

    displayResults();
}

function displayResults() {
    if (!segmentationResults) return;

    // Update summary cards
    document.getElementById('clusterCount').textContent = segmentationResults.numClusters;
    document.getElementById('totalCustomers').textContent = segmentationResults.totalCustomers;
    document.getElementById('avgClusterSize').textContent = 
        Math.round(segmentationResults.totalCustomers / segmentationResults.numClusters);
    document.getElementById('silhouetteScore').textContent = segmentationResults.silhouetteScore.toFixed(2);

    // Display cluster cards
    const clustersContainer = document.getElementById('clustersContainer');
    let html = '';

    segmentationResults.clusters.forEach(cluster => {
        html += `
            <div class="cluster-card">
                <div class="cluster-title">Segment ${cluster.id}</div>
                <div class="cluster-stat">
                    <div class="cluster-stat-label">Size</div>
                    <div class="cluster-stat-value">${cluster.size} customers</div>
                </div>
                <div class="cluster-stat">
                    <div class="cluster-stat-label">Avg Age</div>
                    <div class="cluster-stat-value">${cluster.avgAge} years</div>
                </div>
                <div class="cluster-stat">
                    <div class="cluster-stat-label">Avg Spend</div>
                    <div class="cluster-stat-value">$${cluster.avgSpend}</div>
                </div>
                <div class="cluster-stat">
                    <div class="cluster-stat-label">Top Category</div>
                    <div class="cluster-stat-value">${cluster.topCategory}</div>
                </div>
                <div class="cluster-size">${cluster.description}</div>
            </div>
        `;
    });

    clustersContainer.innerHTML = html;

    // Enable download buttons
    document.querySelectorAll('[onclick*="downloadResults"]').forEach(btn => {
        btn.disabled = false;
    });
}

function downloadResults(format) {
    if (!segmentationResults) {
        showMessage('❌ No results to download', 'error');
        return;
    }

    let content = '';
    let filename = '';

    if (format === 'csv') {
        filename = 'segmentation_results.csv';
        content = 'Segment,Size,AvgAge,AvgSpend,TopCategory,Description\n';
        segmentationResults.clusters.forEach(cluster => {
            content += `${cluster.id},${cluster.size},${cluster.avgAge},${cluster.avgSpend},"${cluster.topCategory}","${cluster.description}"\n`;
        });
    } else if (format === 'json') {
        filename = 'segmentation_results.json';
        content = JSON.stringify(segmentationResults, null, 2);
    } else if (format === 'pdf') {
        showMessage('📄 PDF download feature coming soon!');
        return;
    }

    // Create download link
    const blob = new Blob([content], { type: 'text/plain' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    window.URL.revokeObjectURL(url);
    document.body.removeChild(a);

    showMessage(`✅ Downloaded ${filename}`);
}

// ===========================
// VISUALIZATION Functions
// ===========================
function initializeCharts() {
    // Cluster Distribution Chart
    if (document.getElementById('clusterChart')) {
        const clusterCtx = document.getElementById('clusterChart').getContext('2d');
        
        if (charts.clusterChart) {
            charts.clusterChart.destroy();
        }

        charts.clusterChart = new Chart(clusterCtx, {
            type: 'bar',
            data: {
                labels: ['Segment 0', 'Segment 1', 'Segment 2', 'Segment 3'],
                datasets: [{
                    label: 'Number of Customers',
                    data: sampleClusters,
                    backgroundColor: [
                        'rgba(99, 102, 241, 0.8)',
                        'rgba(59, 130, 246, 0.8)',
                        'rgba(16, 185, 129, 0.8)',
                        'rgba(245, 158, 11, 0.8)'
                    ],
                    borderColor: [
                        'rgb(99, 102, 241)',
                        'rgb(59, 130, 246)',
                        'rgb(16, 185, 129)',
                        'rgb(245, 158, 11)'
                    ],
                    borderWidth: 2,
                    borderRadius: 8
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        max: 150
                    }
                }
            }
        });
    }

    // Scatter Plot: Spend vs Recency
    if (document.getElementById('scatterChart')) {
        const scatterCtx = document.getElementById('scatterChart').getContext('2d');
        
        if (charts.scatterChart) {
            charts.scatterChart.destroy();
        }

        const colors = ['rgba(99, 102, 241, 0.7)', 'rgba(59, 130, 246, 0.7)', 'rgba(16, 185, 129, 0.7)', 'rgba(245, 158, 11, 0.7)'];
        const datasets = [0, 1, 2, 3].map(segment => {
            return {
                label: `Segment ${segment}`,
                data: sampleScatterData.filter(d => d.segment === segment).map(d => ({ x: d.x, y: d.y })),
                backgroundColor: colors[segment],
                borderColor: colors[segment].replace('0.7', '1'),
                borderWidth: 2,
                pointRadius: 6,
                pointHoverRadius: 8
            };
        });

        charts.scatterChart = new Chart(scatterCtx, {
            type: 'scatter',
            data: {
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top'
                    }
                },
                scales: {
                    x: {
                        title: {
                            display: true,
                            text: 'Recency (days)'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: 'Spend ($)'
                        }
                    }
                }
            }
        });
    }

    // Age Distribution Chart
    if (document.getElementById('ageChart')) {
        const ageCtx = document.getElementById('ageChart').getContext('2d');
        
        if (charts.ageChart) {
            charts.ageChart.destroy();
        }

        charts.ageChart = new Chart(ageCtx, {
            type: 'line',
            data: {
                labels: ['20-30', '30-40', '40-50', '50-60', '60-70', '70+'],
                datasets: [
                    {
                        label: 'Segment 0',
                        data: [10, 25, 45, 30, 8, 2],
                        borderColor: 'rgb(99, 102, 241)',
                        backgroundColor: 'rgba(99, 102, 241, 0.1)',
                        tension: 0.4,
                        fill: true,
                        borderWidth: 3
                    },
                    {
                        label: 'Segment 1',
                        data: [15, 35, 25, 15, 8, 2],
                        borderColor: 'rgb(59, 130, 246)',
                        backgroundColor: 'rgba(59, 130, 246, 0.1)',
                        tension: 0.4,
                        fill: true,
                        borderWidth: 3
                    },
                    {
                        label: 'Segment 2',
                        data: [20, 30, 15, 10, 5, 0],
                        borderColor: 'rgb(16, 185, 129)',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        tension: 0.4,
                        fill: true,
                        borderWidth: 3
                    },
                    {
                        label: 'Segment 3',
                        data: [2, 8, 15, 25, 35, 15],
                        borderColor: 'rgb(245, 158, 11)',
                        backgroundColor: 'rgba(245, 158, 11, 0.1)',
                        tension: 0.4,
                        fill: true,
                        borderWidth: 3
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top'
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }

    // Radar Chart: Cluster Centers
    if (document.getElementById('radarChart')) {
        const radarCtx = document.getElementById('radarChart').getContext('2d');
        
        if (charts.radarChart) {
            charts.radarChart.destroy();
        }

        charts.radarChart = new Chart(radarCtx, {
            type: 'radar',
            data: {
                labels: ['Age', 'Spend', 'Frequency', 'Recency', 'Engagement'],
                datasets: [
                    {
                        label: 'Segment 0',
                        data: [85, 95, 75, 65, 80],
                        borderColor: 'rgb(99, 102, 241)',
                        backgroundColor: 'rgba(99, 102, 241, 0.2)',
                        borderWidth: 2
                    },
                    {
                        label: 'Segment 1',
                        data: [60, 55, 70, 75, 60],
                        borderColor: 'rgb(59, 130, 246)',
                        backgroundColor: 'rgba(59, 130, 246, 0.2)',
                        borderWidth: 2
                    },
                    {
                        label: 'Segment 2',
                        data: [35, 40, 50, 85, 45],
                        borderColor: 'rgb(16, 185, 129)',
                        backgroundColor: 'rgba(16, 185, 129, 0.2)',
                        borderWidth: 2
                    },
                    {
                        label: 'Segment 3',
                        data: [90, 98, 70, 50, 85],
                        borderColor: 'rgb(245, 158, 11)',
                        backgroundColor: 'rgba(245, 158, 11, 0.2)',
                        borderWidth: 2
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top'
                    }
                },
                scales: {
                    r: {
                        beginAtZero: true,
                        max: 100
                    }
                }
            }
        });
    }
}

// ===========================
// Utility Functions
// ===========================
function showMessage(message, type = 'success') {
    // Create a simple notification
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 90px;
        right: 20px;
        background-color: ${type === 'error' ? '#ef4444' : '#10b981'};
        color: white;
        padding: 15px 20px;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.2);
        z-index: 10000;
        max-width: 400px;
        font-weight: 600;
    `;
    notification.textContent = message;
    document.body.appendChild(notification);

    setTimeout(() => {
        notification.style.opacity = '0';
        notification.style.transition = 'opacity 0.3s ease';
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}

// Initialize page on load
document.addEventListener('DOMContentLoaded', () => {
    navigateTo('home');
    addLog('[INFO] Dashboard initialized successfully');
});

// Handle browser back button for navigation
window.addEventListener('popstate', (e) => {
    const page = new URLSearchParams(window.location.search).get('page') || 'home';
    navigateTo(page);
});
