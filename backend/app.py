"""
Flask Backend for E-Commerce Customer Segmentation Dashboard
Handles file uploads, job management, and Spark integration
"""

import os
import json
import uuid
import threading
import subprocess
import time
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
from spark_job import run_segmentation_pipeline

# ===========================
# Flask Configuration
# ===========================
app = Flask(__name__)
CORS(app)

# Directory Configuration
BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / 'uploads'
RESULTS_DIR = BASE_DIR / 'results'
STATUS_DIR = BASE_DIR / 'status'

# Create directories if they don't exist
UPLOAD_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)
STATUS_DIR.mkdir(exist_ok=True)

# File Configuration
ALLOWED_EXTENSIONS = {'csv'}
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE
app.config['UPLOAD_FOLDER'] = str(UPLOAD_DIR)

# Job tracking dictionary (in-memory)
jobs = {}

# ===========================
# Helper Functions
# ===========================
def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def update_job_status(job_id, stage, progress, message):
    """Update job status JSON file and in-memory dictionary"""
    status_data = {
        'job_id': job_id,
        'stage': stage,
        'progress': progress,
        'message': message,
        'timestamp': datetime.now().isoformat()
    }
    
    # Update in-memory
    jobs[job_id] = status_data
    
    # Write to file
    status_file = STATUS_DIR / f'{job_id}.json'
    with open(status_file, 'w') as f:
        json.dump(status_data, f, indent=2)


def run_spark_job_async(job_id, filepath, filename):
    """Run Spark job in background thread"""
    try:
        update_job_status(job_id, 'initializing', 5, 'Starting job...')
        time.sleep(0.5)
        
        # Call PySpark pipeline
        results = run_segmentation_pipeline(
            job_id=job_id,
            filepath=str(filepath),
            results_dir=str(RESULTS_DIR),
            status_dir=str(STATUS_DIR)
        )
        
        # Mark job as completed
        update_job_status(job_id, 'completed', 100, 'Job completed successfully')
        
    except Exception as e:
        print(f"Error in Spark job {job_id}: {str(e)}")
        update_job_status(job_id, 'error', 0, f'Error: {str(e)}')


# ===========================
# API ROUTES
# ===========================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'E-Commerce Segmentation Backend'
    }), 200


@app.route('/upload', methods=['POST'])
def upload_file():
    """
    POST /upload
    Accept CSV file upload
    Returns: { "status": "uploaded", "filename": "customers.csv" }
    """
    try:
        # Check if file is in request
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Only CSV files are allowed'}), 400
        
        # Generate unique filename with timestamp
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_')
        unique_filename = timestamp + filename
        filepath = UPLOAD_DIR / unique_filename
        
        # Save file
        file.save(str(filepath))
        
        return jsonify({
            'status': 'uploaded',
            'filename': unique_filename,
            'message': f'File uploaded successfully: {unique_filename}'
        }), 200
    
    except Exception as e:
        print(f"Upload error: {str(e)}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500


@app.route('/run-job', methods=['POST'])
def run_job():
    """
    POST /run-job
    Input: { "filename": "customers.csv" }
    Launch PySpark job in background
    Returns: { "status": "started", "job_id": "job_123" }
    """
    try:
        data = request.get_json()
        
        if not data or 'filename' not in data:
            return jsonify({'error': 'filename is required'}), 400
        
        filename = secure_filename(data['filename'])
        filepath = UPLOAD_DIR / filename
        
        # Check if file exists
        if not filepath.exists():
            return jsonify({'error': f'File not found: {filename}'}), 404
        
        # Generate unique job ID
        job_id = f"job_{uuid.uuid4().hex[:12]}"
        
        # Initialize job status
        update_job_status(job_id, 'queued', 0, 'Job queued for processing')
        
        # Start job in background thread
        thread = threading.Thread(
            target=run_spark_job_async,
            args=(job_id, filepath, filename),
            daemon=True
        )
        thread.start()
        
        return jsonify({
            'status': 'started',
            'job_id': job_id,
            'message': 'Segmentation job started'
        }), 200
    
    except Exception as e:
        print(f"Job start error: {str(e)}")
        return jsonify({'error': f'Failed to start job: {str(e)}'}), 500


@app.route('/job-status/<job_id>', methods=['GET'])
def job_status(job_id):
    """
    GET /job-status/<job_id>
    Read progress file from backend/status/job_123.json
    Returns: {
        "stage": "processing",
        "progress": 45,
        "message": "Extracting features..."
    }
    """
    try:
        status_file = STATUS_DIR / f'{job_id}.json'
        
        if not status_file.exists():
            return jsonify({'error': f'Job not found: {job_id}'}), 404
        
        with open(status_file, 'r') as f:
            status_data = json.load(f)
        
        return jsonify(status_data), 200
    
    except Exception as e:
        print(f"Status check error: {str(e)}")
        return jsonify({'error': f'Failed to get status: {str(e)}'}), 500


@app.route('/segments', methods=['GET'])
def get_segments():
    """
    GET /segments
    Returns cluster information and segment profiles
    Returns: {
        "numClusters": 4,
        "totalCustomers": 290,
        "clusters": [{ id, size, avgAge, avgSpend, topCategory, description }, ...],
        "silhouetteScore": 0.72
    }
    """
    try:
        segment_file = RESULTS_DIR / 'segment_profiles.json'
        
        if not segment_file.exists():
            return jsonify({
                'error': 'No segmentation results available',
                'message': 'Run a segmentation job first'
            }), 404
        
        with open(segment_file, 'r') as f:
            segments_data = json.load(f)
        
        return jsonify(segments_data), 200
    
    except Exception as e:
        print(f"Segments error: {str(e)}")
        return jsonify({'error': f'Failed to get segments: {str(e)}'}), 500


@app.route('/cluster-plot-data', methods=['GET'])
def get_cluster_plot_data():
    """
    GET /cluster-plot-data
    Return visualization arrays:
    - cluster_counts (bar chart)
    - scatter_plot (scatter plot)
    - line_chart (trend)
    - radar_chart (cluster profiles)
    """
    try:
        chart_file = RESULTS_DIR / 'chart_data.json'
        
        if not chart_file.exists():
            return jsonify({
                'error': 'No chart data available',
                'message': 'Run a segmentation job first'
            }), 404
        
        with open(chart_file, 'r') as f:
            chart_data = json.load(f)
        
        return jsonify(chart_data), 200
    
    except Exception as e:
        print(f"Chart data error: {str(e)}")
        return jsonify({'error': f'Failed to get chart data: {str(e)}'}), 500


@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    """
    GET /download/<filename>
    Serve files from backend/results/
    """
    try:
        filename = secure_filename(filename)
        filepath = RESULTS_DIR / filename
        
        if not filepath.exists():
            return jsonify({'error': f'File not found: {filename}'}), 404
        
        return send_file(
            str(filepath),
            as_attachment=True,
            download_name=filename
        )
    
    except Exception as e:
        print(f"Download error: {str(e)}")
        return jsonify({'error': f'Failed to download file: {str(e)}'}), 500


@app.route('/spark-ui', methods=['GET'])
def spark_ui():
    """
    GET /spark-ui
    Redirect to Spark UI
    """
    return jsonify({
        'spark_ui_url': 'http://localhost:4040',
        'message': 'Open this URL in your browser to view Spark UI'
    }), 200


@app.route('/results-list', methods=['GET'])
def results_list():
    """
    GET /results-list
    List all available result files
    """
    try:
        files = []
        for file in RESULTS_DIR.iterdir():
            if file.is_file():
                files.append({
                    'filename': file.name,
                    'size': file.stat().st_size,
                    'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
                })
        
        return jsonify({
            'files': files,
            'total': len(files)
        }), 200
    
    except Exception as e:
        print(f"Results list error: {str(e)}")
        return jsonify({'error': f'Failed to list results: {str(e)}'}), 500


@app.route('/jobs', methods=['GET'])
def list_jobs():
    """
    GET /jobs
    List all jobs and their statuses
    """
    try:
        job_list = []
        for status_file in STATUS_DIR.glob('*.json'):
            with open(status_file, 'r') as f:
                job_data = json.load(f)
                job_list.append(job_data)
        
        return jsonify({
            'jobs': job_list,
            'total': len(job_list)
        }), 200
    
    except Exception as e:
        print(f"Jobs list error: {str(e)}")
        return jsonify({'error': f'Failed to list jobs: {str(e)}'}), 500


@app.route('/cleanup', methods=['POST'])
def cleanup():
    """
    POST /cleanup
    Clean up old results and status files
    """
    try:
        cleaned = []
        
        # Clean results
        for file in RESULTS_DIR.glob('*'):
            if file.is_file() and file.stat().st_size == 0:
                file.unlink()
                cleaned.append(file.name)
        
        # Clean status
        for file in STATUS_DIR.glob('*.json'):
            if file.is_file():
                # Keep recent files (1 hour)
                mtime = file.stat().st_mtime
                age = time.time() - mtime
                if age > 3600:  # Older than 1 hour
                    file.unlink()
                    cleaned.append(file.name)
        
        return jsonify({
            'status': 'success',
            'cleaned_files': cleaned,
            'message': f'Cleaned up {len(cleaned)} files'
        }), 200
    
    except Exception as e:
        print(f"Cleanup error: {str(e)}")
        return jsonify({'error': f'Cleanup failed: {str(e)}'}), 500


# ===========================
# Error Handlers
# ===========================

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


@app.errorhandler(413)
def request_entity_too_large(error):
    return jsonify({'error': 'File is too large (max 100 MB)'}), 413


# ===========================
# Main Entry Point
# ===========================

if __name__ == '__main__':
    print("=" * 60)
    print("E-Commerce Customer Segmentation Backend")
    print("=" * 60)
    print(f"Upload Directory: {UPLOAD_DIR}")
    print(f"Results Directory: {RESULTS_DIR}")
    print(f"Status Directory: {STATUS_DIR}")
    print("=" * 60)
    print("Starting Flask server on http://localhost:5000")
    print("CORS enabled for frontend integration")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
