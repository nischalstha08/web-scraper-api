"""
app.py - Flask API Module

A secure, efficient API to serve scraped data with proper rate limiting,
authentication, and error handling.
"""

import os
import logging
import json
from typing import Dict, List, Any
from pathlib import Path
from datetime import datetime
from threading import Thread
from functools import wraps

from flask import Flask, jsonify, request, abort, make_response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_cors import CORS
import jwt
from dotenv import load_dotenv

from scheduler import Scheduler
from scraper import WebScraper

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("flask_api")

# Initialize Flask application
app = Flask(__name__)
app.config.update({
    'SECRET_KEY': os.getenv('SECRET_KEY', 'super-secret-key'),
    'DATA_DIR': os.getenv('DATA_DIR', 'scraped_data'),
    'ENABLE_AUTH': os.getenv('ENABLE_AUTH', 'false').lower() == 'true'
})

# Security middleware
CORS(app, resources={r"/*": {"origins": os.getenv('ALLOWED_ORIGINS', '*')}})
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)

# Initialize scheduler
scheduler = Scheduler(
    state_file=os.getenv('SCHEDULER_STATE', 'scheduler_state.json'),
    max_workers=int(os.getenv('MAX_WORKERS', 5))
)

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not app.config['ENABLE_AUTH']:
            return f(*args, **kwargs)
            
        token = request.headers.get('Authorization')
        if not token:
            abort(401, 'Authorization token is missing')
        
        try:
            token = token.split(" ")[1]  # Bearer <token>
            jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
        except (IndexError, jwt.ExpiredSignatureError, jwt.InvalidTokenError) as e:
            logger.warning(f"Invalid token attempt: {str(e)}")
            abort(401, 'Invalid or expired token')
        
        return f(*args, **kwargs)
    return decorated

@app.route('/api/data/<dataset>', methods=['GET'])
@token_required
@limiter.limit("10/minute")
def get_data(dataset: str):
    """Retrieve scraped data from stored JSON files"""
    try:
        data_dir = Path(app.config['DATA_DIR'])
        files = sorted(data_dir.glob(f"{dataset}_*.json"), reverse=True)
        
        if not files:
            abort(404, f"No data found for {dataset}")
            
        with open(files[0], 'r') as f:
            data = json.load(f)
            
        return jsonify({
            "dataset": dataset,
            "count": len(data),
            "results": data[:100],  # Return first 100 items by default
            "timestamp": files[0].stem.split('_')[-1]
        })
    
    except Exception as e:
        logger.error(f"Data retrieval error: {str(e)}")
        abort(500, "Failed to retrieve data")

@app.route('/api/scheduler/status', methods=['GET'])
@token_required
def get_scheduler_status():
    """Get current scheduler status and task list"""
    try:
        return jsonify({
            "status": "running",
            "tasks": scheduler.get_all_task_status()
        })
    except Exception as e:
        logger.error(f"Scheduler status error: {str(e)}")
        abort(500, "Failed to get scheduler status")

@app.route('/api/scheduler/trigger/<task_name>', methods=['POST'])
@token_required
@limiter.limit("1/minute")
def trigger_task(task_name: str):
    """Manually trigger a scheduled task"""
    try:
        if scheduler.run_task_now(task_name):
            return jsonify({"status": "queued", "task": task_name})
        abort(404, f"Task {task_name} not found")
    except Exception as e:
        logger.error(f"Task trigger error: {str(e)}")
        abort(500, "Failed to trigger task")

@app.errorhandler(404)
def resource_not_found(e):
    return jsonify(error=str(e)), 404

@app.errorhandler(429)
def rate_limit_exceeded(e):
    return jsonify(error="Rate limit exceeded"), 429

@app.errorhandler(500)
def internal_error(e):
    return jsonify(error="Internal server error"), 500

def run_scheduler():
    """Run scheduler in background thread"""
    logger.info("Starting scheduler thread")
    scheduler.start()
    
    # Register example tasks (should be moved to separate config)
    try:
        from tasks import register_scraping_tasks
        register_scraping_tasks(scheduler)
    except ImportError:
        logger.warning("No custom tasks found. Using demo tasks.")
        register_demo_tasks(scheduler)

def register_demo_tasks(scheduler: Scheduler):
    """Register example scraping tasks"""
    def demo_product_scraper():
        with app.app_context():
            logger.info("Running demo product scraper")
            scraper = WebScraper(rate_limit=2.0)
            # Add actual scraping logic here
            
    scheduler.register_task(
        name="demo_product_scrape",
        function=demo_product_scraper,
        interval=3600  # Run hourly
    )

if __name__ == '__main__':
    # Start scheduler thread
    scheduler_thread = Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Start Flask app
    app.run(
        host=os.getenv('HOST', '0.0.0.0'),
        port=int(os.getenv('PORT', 5000)),
        debug=os.getenv('DEBUG', 'false').lower() == 'true'
    )
