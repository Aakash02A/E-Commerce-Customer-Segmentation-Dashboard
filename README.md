# 📊 E-Commerce Customer Segmentation Dashboard

A complete, production-ready system for segmenting e-commerce customers using Apache Spark and advanced machine learning. This full-stack application combines a powerful Flask backend with an intuitive web-based dashboard to unlock actionable insights from customer data.

---

## 🎯 Features

- **🔥 Apache Spark Integration** - Scalable distributed processing for large datasets
- **🤖 Machine Learning** - KMeans clustering with feature engineering and normalization
- **📊 Interactive Dashboard** - 6-page responsive web interface with visualizations
- **📁 Easy Data Upload** - Simple CSV file upload with validation
- **⚡ Real-time Job Monitoring** - Track segmentation jobs with live progress updates
- **📈 Rich Visualizations** - Interactive charts and detailed segmentation analytics
- **🔒 CORS-Enabled** - Secure frontend-backend communication
- **📱 Responsive Design** - Works on desktop, tablet, and mobile devices

---

## 📋 Project Structure

```
├── backend/                          # Flask API server
│   ├── app.py                        # Main Flask application with 11 API endpoints
│   ├── spark_job.py                  # PySpark ML pipeline for customer segmentation
│   ├── requirements.txt               # Python dependencies
│   ├── sample_data.csv               # 30 sample customer records for testing
│   └── uploads/                      # Directory for user-uploaded files
│       └── results/                  # Output directory for segmentation results
│       └── status/                   # Job status tracking files
│
├── frontend/                         # Web dashboard
│   ├── index.html                    # Main dashboard interface (426 lines)
│   ├── serve.py                      # HTTP server for frontend
│   ├── sample_customers.csv          # Sample data for reference
│   └── assets/
│       ├── css/
│       │   └── style.css             # Dashboard styling
│       └── js/
│           └── app.js                # Frontend logic and API integration
│
├── README.md                         # This file
├── LICENSE                           # MIT License
└── startup info.md                   # Quick start guide

```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.8 or higher
- Apache Spark 3.5.0
- Modern web browser

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Aakash02A/E-Commerce-Customer-Segmentation-Dashboard.git
   cd E-Commerce-Customer-Segmentation-Dashboard
   ```

2. **Install Python dependencies**
   ```bash
   cd backend
   pip install -r requirements.txt
   ```

3. **Start the backend server**
   ```bash
   python app.py
   ```
   Backend will run at `http://localhost:5000`

4. **In a new terminal, start the frontend server**
   ```bash
   cd frontend
   python serve.py
   ```
   Frontend will run at `http://localhost:8000`

5. **Open your browser**
   ```
   http://localhost:8000
   ```

---

## 🎮 Usage Guide

### 1. **Home Page**
   - View system status and Spark connection
   - Quick start guide with 3-step process
   - Feature overview and capabilities

### 2. **Upload Data**
   - Upload CSV file with customer data
   - Required columns: Age, Spend, Recency, Frequency
   - Supports up to 100 MB files
   - Automatic validation and error handling

### 3. **Job Monitor**
   - Real-time progress tracking for segmentation jobs
   - View job status, stage, and completion percentage
   - Download results when complete
   - Historical job tracking

### 4. **Segmentation Results**
   - Detailed breakdown of customer segments
   - Segment characteristics and metrics
   - Customer count per segment
   - Export results to CSV

### 5. **Visualization**
   - Interactive charts and graphs
   - Segment distribution analysis
   - Feature-based visualizations
   - Multi-dimensional insights

### 6. **About**
   - Project information and documentation
   - Technology stack overview
   - Contact and support information

---

## 📦 Technology Stack

### Backend
| Technology | Version | Purpose |
|-----------|---------|---------|
| **Flask** | 2.3.3 | REST API framework |
| **PySpark** | 3.5.0 | Distributed data processing |
| **Pandas** | 2.0.3 | Data manipulation |
| **Scikit-learn** | 1.3.0 | ML utilities |
| **Flask-CORS** | 4.0.0 | Cross-origin support |

### Frontend
| Technology | Purpose |
|-----------|---------|
| **HTML5** | Semantic markup |
| **CSS3** | Responsive styling |
| **JavaScript (ES6+)** | Client-side logic |
| **Chart.js** | Interactive visualizations |

---

## 🔧 API Endpoints

### Core Endpoints
- `GET /health` - Health check
- `POST /upload` - Upload CSV file
- `POST /segmentation-start` - Start segmentation job
- `GET /job-status/<job_id>` - Get job status
- `GET /segmentation-results/<job_id>` - Get results
- `GET /download-results/<job_id>` - Download CSV results
- `GET /jobs` - List all jobs
- `GET /segment-summary/<job_id>` - Get segment summary
- `GET /segment-metrics/<job_id>` - Get detailed metrics
- `POST /validate-file` - Validate CSV structure
- `GET /sample-data` - Get sample dataset

---

## 🧠 Machine Learning Pipeline

The PySpark ML pipeline performs the following steps:

1. **Data Loading** - Read CSV file into Spark DataFrame
2. **Data Validation** - Check for required columns (Age, Spend, Recency, Frequency)
3. **Preprocessing** - Handle missing values, remove outliers, normalize column names
4. **Feature Engineering** - Create feature vectors from numeric columns
5. **Scaling** - Standardize features using StandardScaler
6. **Clustering** - Apply KMeans algorithm (default k=3 clusters)
7. **Evaluation** - Calculate silhouette score and other metrics
8. **Results Export** - Save predictions and segment summaries to CSV

### Feature Set
- **Age** - Customer age
- **Spend** - Total spending amount
- **Recency** - Days since last purchase
- **Frequency** - Purchase frequency

---

## 📊 Sample Data Format

Upload CSV files with the following structure:

```csv
CustomerID,Age,Gender,Spend,Recency,Frequency
CUST001,28,M,2500,15,8
CUST002,45,F,1800,30,5
CUST003,35,M,3200,7,12
...
```

**Required Columns:**
- Age (numeric)
- Spend (numeric)
- Recency (numeric)
- Frequency (numeric)

**Optional Columns:**
- CustomerID (identifier)
- Gender (categorical)
- Any other customer attributes

---

## 🛠️ Configuration

### Backend Configuration (app.py)
```python
UPLOAD_DIR = 'uploads'           # Upload directory
RESULTS_DIR = 'results'          # Results directory
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB limit
ALLOWED_EXTENSIONS = {'csv'}     # Allowed file types
```

### Spark Configuration (spark_job.py)
```python
SPARK_APP_NAME = "CustomerSegmentation"
NUMERIC_FEATURES = ['Age', 'Spend', 'Recency', 'Frequency']
DEFAULT_K_CLUSTERS = 3           # Number of segments
```

---

## 📝 Job Status Tracking

Jobs are tracked through JSON status files stored in `backend/status/`:

```json
{
  "job_id": "abc123def456",
  "stage": "clustering",
  "progress": 75,
  "message": "Running KMeans clustering...",
  "timestamp": "2025-12-10T14:30:45.123456"
}
```

Stages include:
- **loading** - Initial data loading
- **preprocessing** - Data cleaning and validation
- **feature_engineering** - Creating feature vectors
- **scaling** - Normalizing features
- **clustering** - Running KMeans algorithm
- **evaluation** - Computing metrics
- **saving** - Exporting results
- **completed** - Job finished successfully
- **error** - Job failed with error message

---

## 🐛 Troubleshooting

### Issue: Backend won't start
**Solution:** 
- Ensure Python 3.8+ is installed
- Run `pip install -r requirements.txt`
- Check that port 5000 is not in use
- Verify all dependencies installed: `pip list | grep Flask`

### Issue: Spark job fails
**Solution:**
- Ensure Spark 3.5.0 is installed
- Check Java is installed: `java -version`
- Verify CSV has required columns: Age, Spend, Recency, Frequency
- Check file size is under 100 MB

### Issue: Frontend shows "Connection failed"
**Solution:**
- Verify backend is running on http://localhost:5000
- Check browser console for errors (F12)
- Ensure CORS is enabled in Flask
- Try clearing browser cache

### Issue: Results not showing
**Solution:**
- Wait for job to complete (check Job Monitor)
- Verify data uploaded has at least 3 rows
- Check backend logs for processing errors
- Ensure write permissions on results directory

---

## 📈 Performance Considerations

- **Small datasets (<1000 rows):** 5-10 seconds
- **Medium datasets (1K-100K rows):** 20-60 seconds
- **Large datasets (100K+ rows):** 2-5+ minutes
- **Max file size:** 100 MB
- **Optimal cluster count:** 3-5 segments for most use cases

---

## 🔐 Security Features

- **CORS Protection** - Cross-origin requests properly managed
- **File Validation** - CSV files validated before processing
- **Size Limits** - Maximum file size enforced (100 MB)
- **Extension Whitelist** - Only CSV files allowed
- **Path Sanitization** - Secure filename handling with `secure_filename()`
- **Error Handling** - Detailed errors logged, generic errors sent to frontend

---

<<<<<<< HEAD
=======

>>>>>>> 7f0b0a5e2b7197c6b7584f76e0eeaa42419dd0b2
## 🎓 Learning Resources

This project demonstrates:
- **Flask** - Building REST APIs with Python
- **PySpark** - Distributed computing and ML pipelines
- **Machine Learning** - Customer segmentation with KMeans
- **Full-Stack Development** - Frontend-backend integration
- **Data Engineering** - CSV processing and validation
- **Web Development** - Responsive HTML/CSS/JavaScript

<<<<<<< HEAD
---
=======
>>>>>>> 7f0b0a5e2b7197c6b7584f76e0eeaa42419dd0b2
