"""
scheduler.py - Task Scheduling Module

A robust and flexible scheduler for automating web scraping tasks
with support for various scheduling patterns, persistence, and failure handling.
"""

import logging
import time
import os
import signal
import json
import pickle
from typing import Dict, List, Any, Callable, Optional, Union, Tuple
from datetime import datetime, timedelta
import threading
import queue
import hashlib
from concurrent.futures import ThreadPoolExecutor
import traceback
from croniter import croniter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scheduler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("task_scheduler")

class Task:
    """Represents a scheduled scraping task with execution details."""
    
    def __init__(self, 
                 name: str,
                 function: Callable,
                 args: Tuple = None,
                 kwargs: Dict = None,
                 interval: int = None,  # Interval in seconds
                 cron_expression: str = None,  # Simple cron expression
                 next_run: datetime = None,
                 enabled: bool = True,
                 retry_on_failure: bool = True,
                 max_retries: int = 3,
                 retry_delay: int = 300,  # 5 minutes
                 timeout: int = 3600):  # 1 hour default timeout
        """
        Initialize a scheduled task.
        
        Args:
            name: Unique name for the task
            function: Function to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            interval: Time interval between executions in seconds (mutually exclusive with cron_expression)
            cron_expression: Simple cron expression (e.g., "0 9 * * *" for daily at 9am)
            next_run: Next scheduled run time (will be calculated if not provided)
            enabled: Whether the task is enabled
            retry_on_failure: Whether to retry on failure
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
            timeout: Maximum execution time in seconds
        """
        self.name = name
        self.function = function
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.interval = interval
        self.cron_expression = cron_expression
        self.enabled = enabled
        self.retry_on_failure = retry_on_failure
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        
        # Execution stats
        self.last_run = None
        self.last_status = None
        self.last_error = None
        self.run_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.retry_count = 0
        self.running = False
        
        # Set initial next run time
        if next_run:
            self.next_run = next_run
        else:
            self.calculate_next_run()

    #Use croniter for accurate cron scheduling. Bug: Inaccurate cron scheduling

    def calculate_next_run(self, from_time: datetime = None) -> datetime:
        now = from_time or datetime.now()

        if self.interval:
            self.next_run = now + timedelta(seconds=self.interval)
        elif self.cron_expression:
            try:
                base_time = from_time or datetime.now()
                cron = croniter(self.cron_expression, base_time)
                self.next_run = cron.get_next(datetime)
            except Exception as e:
                logger.error(f"Error parsing cron expression for {self.name}: {e}")
                self.next_run = now + timedelta(hours=24) # Default fallback
        else:
            self.next_run = now + timedelta(days=1)
        
        return self.next_run
    
    # def calculate_next_run(self, from_time: datetime = None) -> datetime:
    #     """
    #     Calculate the next run time based on interval or cron expression.
        
    #     Args:
    #         from_time: Base time to calculate from (defaults to now)
            
    #     Returns:
    #         Datetime of next scheduled run
    #     """
    #     now = from_time or datetime.now()
        
    #     if self.interval:
    #         self.next_run = now + timedelta(seconds=self.interval)
    #     elif self.cron_expression:
    #         # Simplified cron implementation - only handles basic patterns
    #         # Format: "minute hour day_of_month month day_of_week"
    #         # For a production system, consider using a library like 'croniter'
    #         try:
    #             parts = self.cron_expression.split()
    #             if len(parts) != 5:
    #                 raise ValueError("Invalid cron expression format")
                
    #             minute, hour, day, month, weekday = parts
                
    #             # Very simplified cron calculation for daily tasks
    #             if minute == "0" and hour.isdigit() and day == "*" and month == "*":
    #                 target = now.replace(hour=int(hour), minute=0, second=0, microsecond=0)
    #                 if target <= now:
    #                     target += timedelta(days=1)
    #                 self.next_run = target
    #             else:
    #                 # Default fallback for complex patterns
    #                 self.next_run = now + timedelta(days=1)
    #                 logger.warning(f"Complex cron pattern for {self.name} - using simplified fallback")
    #         except Exception as e:
    #             logger.error(f"Error parsing cron expression for {self.name}: {e}")
    #             self.next_run = now + timedelta(hours=24)  # Default fallback
    #     else:
    #         # Default to running once a day if no schedule specified
    #         self.next_run = now + timedelta(days=1)
        
    #     return self.next_run
    
    def get_status_dict(self) -> Dict[str, Any]:
        """Get task status as a dictionary for reporting."""
        return {
            "name": self.name,
            "enabled": self.enabled,
            "running": self.running,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "last_status": self.last_status,
            "last_error": self.last_error,
            "run_count": self.run_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "retry_count": self.retry_count,
            "interval": self.interval,
            "cron_expression": self.cron_expression
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary for serialization (excluding function)."""
        return {
            "name": self.name,
            "enabled": self.enabled,
            "interval": self.interval,
            "cron_expression": self.cron_expression,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "retry_on_failure": self.retry_on_failure,
            "max_retries": self.max_retries,
            "retry_delay": self.retry_delay,
            "timeout": self.timeout,
            "stats": {
                "run_count": self.run_count,
                "success_count": self.success_count,
                "failure_count": self.failure_count,
                "retry_count": self.retry_count,
                "last_status": self.last_status,
                "last_error": self.last_error
            }
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], function_resolver: Callable) -> 'Task':
        """
        Create a Task instance from a dictionary.
        
        Args:
            data: Dictionary with task data
            function_resolver: Function to resolve function references
            
        Returns:
            Task instance
        """
        # Get the function based on the name
        function = function_resolver(data["name"])
        
        # Create task instance
        task = cls(
            name=data["name"],
            function=function,
            interval=data.get("interval"),
            cron_expression=data.get("cron_expression"),
            enabled=data.get("enabled", True),
            retry_on_failure=data.get("retry_on_failure", True),
            max_retries=data.get("max_retries", 3),
            retry_delay=data.get("retry_delay", 300),
            timeout=data.get("timeout", 3600)
        )
        
        # Restore datetime objects
        if data.get("next_run"):
            task.next_run = datetime.fromisoformat(data["next_run"])
        if data.get("last_run"):
            task.last_run = datetime.fromisoformat(data["last_run"])
        
        # Restore statistic
        stats = data.get("stats", {})
        task.run_count = stats.get("run_count", 0)
        task.success_count = stats.get("success_count", 0)
        task.failure_count = stats.get("failure_count", 0)
        task.retry_count = stats.get("retry_count", 0)
        task.last_status = stats.get("last_status")
        task.last_error = stats.get("last_error")
        
        return task


class Scheduler:
    """Manages and executes scheduled scraping tasks."""
    
    def __init__(self, 
                 state_file: str = "scheduler_state.json",
                 max_workers: int = 5,
                 check_interval: int = 1):
        """
        Initialize the task scheduler.
        
        Args:
            state_file: File to persist scheduler state
            max_workers: Maximum number of concurrent worker threads
            check_interval: Interval in seconds to check for due tasks
        """
        self.tasks = {}  # Dict of task_name -> Task
        self.state_file = state_file
        self.max_workers = max_workers
        self.check_interval = check_interval
        
        # Threading components
        self.running = False
        self.scheduler_thread = None
        self.worker_pool = None
        self.task_queue = queue.Queue()
        self.lock = threading.RLock()
        
        # Signal handling
        self._setup_signal_handlers()
        
        logger.info(f"Initialized Scheduler with max_workers={max_workers}")
        
        # Try to restore state from file
        self._restore_state()
    
    def _setup_signal_handlers(self):
        """Set up handlers for system signals."""
        # Handle termination signals gracefully
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
    
    def _handle_shutdown_signal(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down scheduler...")
        self.shutdown()
    
    def _restore_state(self):
        """Restore scheduler state from file."""
        if not os.path.exists(self.state_file):
            logger.info(f"No state file found at {self.state_file}")
            return
        
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            
            logger.info(f"Restoring {len(state['tasks'])} tasks from {self.state_file}")
            
            # We can only restore task metadata, not the actual functions
            # Functions must be registered separately using register_task
            self._restored_task_data = state['tasks']
            
            logger.info("Task metadata loaded from state file")
        except Exception as e:
            logger.error(f"Error restoring state: {e}")
    
    def _save_state(self):
        """Save scheduler state to file."""
        try:
            tasks_data = {}
            
            with self.lock:
                for name, task in self.tasks.items():
                    tasks_data[name] = task.to_dict()
            
            state = {
                "timestamp": datetime.now().isoformat(),
                "tasks": tasks_data
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            
            logger.debug(f"Saved state with {len(tasks_data)} tasks to {self.state_file}")
        except Exception as e:
            logger.error(f"Error saving state: {e}")
    
    def register_task(self, 
                      name: str,
                      function: Callable,
                      args: Tuple = None,
                      kwargs: Dict = None,
                      interval: int = None,
                      cron_expression: str = None,
                      enabled: bool = True,
                      retry_on_failure: bool = True,
                      max_retries: int = 3,
                      retry_delay: int = 300,
                      timeout: int = 3600) -> Task:
        """
        Register a new task with the scheduler.
        
        Args:
            name: Unique name for the task
            function: Function to execute
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            interval: Time interval between executions in seconds
            cron_expression: Simple cron expression (e.g., "0 9 * * *" for daily at 9am)
            enabled: Whether the task is enabled initially
            retry_on_failure: Whether to retry the task on failure
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
            timeout: Maximum execution time in seconds
            
        Returns:
            The registered Task object
        """
        with self.lock:
            # Check if we have restored data for this task
            restored_data = None
            if hasattr(self, '_restored_task_data') and name in getattr(self, '_restored_task_data', {}):
                restored_data = self._restored_task_data[name]
                logger.info(f"Found restored data for task {name}")
            
            # Create new task
            task = Task(
                name=name,
                function=function,
                args=args,
                kwargs=kwargs,
                interval=interval,
                cron_expression=cron_expression,
                enabled=enabled,
                retry_on_failure=retry_on_failure,
                max_retries=max_retries,
                retry_delay=retry_delay,
                timeout=timeout
            )
            
            # Restore state if available
            if restored_data:
                # Update scheduling parameters from restored data
                task.next_run = datetime.fromisoformat(restored_data["next_run"]) if restored_data.get("next_run") else task.next_run
                task.last_run = datetime.fromisoformat(restored_data["last_run"]) if restored_data.get("last_run") else None
                task.run_count = restored_data.get("stats", {}).get("run_count", 0)
                task.success_count = restored_data.get("stats", {}).get("success_count", 0)
                task.failure_count = restored_data.get("stats", {}).get("failure_count", 0)
                task.retry_count = restored_data.get("stats", {}).get("retry_count", 0)
                task.last_status = restored_data.get("stats", {}).get("last_status")
                task.last_error = restored_data.get("stats", {}).get("last_error")
            
            # Store task
            self.tasks[name] = task
            
            logger.info(f"Registered task: {name}, next run: {task.next_run}")
            
            # Save updated state
            self._save_state()
            
            return task
    
    def update_task(self, 
                    name: str, 
                    enabled: bool = None,
                    interval: int = None, 
                    cron_expression: str = None,
                    retry_on_failure: bool = None,
                    max_retries: int = None,
                    retry_delay: int = None,
                    reset_next_run: bool = False) -> Task:
        """
        Update an existing task.
        
        Args:
            name: Name of the task to update
            enabled: New enabled state
            interval: New interval in seconds
            cron_expression: New cron expression
            retry_on_failure: New retry on failure setting
            max_retries: New maximum retries
            retry_delay: New retry delay
            reset_next_run: Whether to recalculate the next run time
            
        Returns:
            Updated Task object
            
        Raises:
            KeyError: If task doesn't exist
        """
        with self.lock:
            if name not in self.tasks:
                raise KeyError(f"Task {name} not found")
            
            task = self.tasks[name]
            
            # Update task parameters
            if enabled is not None:
                task.enabled = enabled
            if interval is not None:
                task.interval = interval
                task.cron_expression = None  # Clear cron if interval is set
            if cron_expression is not None:
                task.cron_expression = cron_expression
                task.interval = None  # Clear interval if cron is set
            if retry_on_failure is not None:
                task.retry_on_failure = retry_on_failure
            if max_retries is not None:
                task.max_retries = max_retries
            if retry_delay is not None:
                task.retry_delay = retry_delay
            
            # Recalculate next run time if requested
            if reset_next_run:
                task.calculate_next_run()
            
            logger.info(f"Updated task: {name}, next run: {task.next_run}")
            
            # Save updated state
            self._save_state()
            
            return task
    
    def remove_task(self, name: str) -> bool:
        """
        Remove a task from the scheduler.
        
        Args:
            name: Name of the task to remove
            
        Returns:
            True if task was removed, False if not found
        """
        with self.lock:
            if name not in self.tasks:
                return False
            
            del self.tasks[name]
            logger.info(f"Removed task: {name}")
            
            # Save updated state
            self._save_state()
            
            return True
    
    def get_task(self, name: str) -> Optional[Task]:
        """
        Get a task by name.
        
        Args:
            name: Name of the task
            
        Returns:
            Task object or None if not found
        """
        with self.lock:
            return self.tasks.get(name)
    
    def get_all_tasks(self) -> Dict[str, Task]:
        """
        Get all registered tasks.
        
        Returns:
            Dict of task_name -> Task
        """
        with self.lock:
            return self.tasks.copy()
    
    def get_task_status(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a task.
        
        Args:
            name: Name of the task
            
        Returns:
            Dict with task status or None if not found
        """
        task = self.get_task(name)
        if not task:
            return None
        
        return task.get_status_dict()
    
    def get_all_task_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Get the status of all tasks.
        
        Returns:
            Dict of task_name -> task_status
        """
        with self.lock:
            return {name: task.get_status_dict() for name, task in self.tasks.items()}
    
    def run_task_now(self, name: str) -> bool:
        """
        Queue a task to run immediately.
        
        Args:
            name: Name of the task
            
        Returns:
            True if task was queued, False if not found or disabled
        """
        with self.lock:
            if name not in self.tasks:
                logger.warning(f"Task {name} not found for immediate execution")
                return False
            
            task = self.tasks[name]
            
            if not task.enabled:
                logger.warning(f"Cannot run disabled task {name}")
                return False
            
            # Queue task for immediate execution
            self.task_queue.put(task)
            logger.info(f"Queued task {name} for immediate execution")
            
            return True
    
    def start(self):
        """Start the scheduler."""
        if self.running:
            logger.warning("Scheduler is already running")
            return
        
        self.running = True
        
        # Create thread pool for workers
        self.worker_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # Start scheduler thread
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        
        logger.info("Scheduler started")
    
    def shutdown(self, wait: bool = True):
        """
        Shutdown the scheduler.
        
        Args:
            wait: Whether to wait for tasks to complete
        """
        if not self.running:
            logger.warning("Scheduler is not running")
            return
        
        logger.info("Shutting down scheduler...")
        self.running = False
        
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=10.0)
        
        if self.worker_pool:
            self.worker_pool.shutdown(wait=wait)
        
        # Save state before exit
        self._save_state()
        
        logger.info("Scheduler stopped")
    
    def _scheduler_loop(self):
        """Main scheduler loop that checks for due tasks."""
        while self.running:
            try:
                self._check_due_tasks()
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                time.sleep(self.check_interval * 2)  # Wait longer after error
    
    def _check_due_tasks(self):
        """Check for and queue tasks that are due for execution."""
        now = datetime.now()
        due_tasks = []
        
        with self.lock:
            for name, task in self.tasks.items():
                if (task.enabled and 
                    task.next_run and 
                    task.next_run <= now and 
                    not task.running):
                    due_tasks.append(task)
        
        if due_tasks:
            logger.debug(f"Found {len(due_tasks)} due tasks")
            
            for task in due_tasks:
                self.task_queue.put(task)
                logger.info(f"Queued due task: {task.name}")
        
        # Process queued tasks
        while not self.task_queue.empty():
            try:
                task = self.task_queue.get(block=False)
                
                with self.lock:
                    # Double check task is not already running
                    if task.running:
                        logger.warning(f"Task {task.name} is already running, skipping")
                        self.task_queue.task_done()
                        continue
                    
                    # Mark task as running
                    task.running = True
                
                # Submit task to worker pool
                self.worker_pool.submit(self._execute_task, task)
                
                self.task_queue.task_done()
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error processing task queue: {e}")
                break
    
    #Replaced recursive retry with a loop-based approach. BUg: Broken Retry Logic

    def _execute_task(self, task: Task):
        task_id = f"{task.name}-{int(time.time())}"
        logger.info(f"Executing task {task.name} (ID: {task_id})")
    
        start_time = time.time()
        error = None
        success = False
        result = None
        retries_remaining = task.max_retries if task.retry_on_failure else 0
    
        while retries_remaining >= 0:
            try:
                # Update task state
                with self.lock:
                    task.last_run = datetime.now()
                    task.run_count += 1
                    self._save_state()
            
                # Execute the task with timeout
                result = task.function(*task.args, **task.kwargs)
                success = True
                break  # Exit loop on success
            
            except Exception as e:
                error = str(e)
                error_tb = traceback.format_exc()
                logger.error(f"Task {task.name} (ID: {task_id}) failed: {e}\n{error_tb}")
            
                if retries_remaining > 0:
                    logger.info(f"Retrying task {task.name} (attempt {task.max_retries - retries_remaining + 1}/{task.max_retries})")
                    time.sleep(task.retry_delay)
                    retries_remaining -= 1
                    with self.lock:
                        task.retry_count += 1
                        self._save_state()
                else:
                    break  # No retries left
    
        # Update task state after execution/retries
        with self.lock:
            task.running = False
            if success:
                task.success_count += 1
                task.last_status = "success"
                task.last_error = None
            else:
                task.failure_count += 1
                task.last_status = "failed"
                task.last_error = error
        
            # Calculate next run time
            task.calculate_next_run()
            self._save_state()
    
        logger.info(f"Task {task.name} next run scheduled for {task.next_run}")




    # def _execute_task(self, task: Task):
    #     """
    #     Execute a scheduled task.
        
    #     Args:
    #         task: Task to execute
    #     """
    #     task_id = f"{task.name}-{int(time.time())}"
    #     logger.info(f"Executing task {task.name} (ID: {task_id})")
        
    #     start_time = time.time()
    #     error = None
    #     success = False
    #     result = None
    #     retry_count = 0
        
    #     try:
    #         # Update task state
    #         with self.lock:
    #             task.last_run = datetime.now()
    #             task.run_count += 1
    #             self._save_state()
            
    #         # Execute the task with timeout
    #         result = task.function(*task.args, **task.kwargs)
            
    #         success = True
            
    #         logger.info(f"Task {task.name} (ID: {task_id}) completed successfully in {time.time() - start_time:.2f}s")
        
    #     except Exception as e:
    #         error = str(e)
    #         error_tb = traceback.format_exc()
    #         logger.error(f"Task {task.name} (ID: {task_id}) failed: {e}\n{error_tb}")
            
    #         # Retry logic
    #         if task.retry_on_failure and retry_count < task.max_retries:
    #             retry_count += 1
    #             logger.info(f"Retrying task {task.name} (attempt {retry_count}/{task.max_retries})")
                
    #             # Wait before retry
    #             time.sleep(task.retry_delay)
                
    #             # Recursive retry with updated counter
    #             with self.lock:
    #                 task.retry_count += 1
    #                 self._save_state()
                
    #             # Note: In a production system, we might want to use a proper
    #             # retry mechanism instead of recursion to avoid stack overflow
        
    #     finally:
    #         # Update task state
    #         with self.lock:
    #             task.running = False
                
    #             if success:
    #                 task.success_count += 1
    #                 task.last_status = "success"
    #                 task.last_error = None
    #             else:
    #                 task.failure_count += 1
    #                 task.last_status = "failed"
    #                 task.last_error = error
                
    #             # Calculate next run time
    #             task.calculate_next_run()
                
    #             # Save updated state
    #             self._save_state()
            
    #         logger.info(f"Task {task.name} next run scheduled for {task.next_run}")

# Example usage
if __name__ == "__main__":
    import scraper
    
    # Create a scheduler
    scheduler = Scheduler(max_workers=3)
    
    # Define some example scraping tasks
    def scrape_products():
        """Example task to scrape product listings."""
        logger.info("Starting product scraping task")
        web_scraper = scraper.WebScraper(rate_limit=2.0)
        
        try:
            products = web_scraper.paginate(
                start_url="https://example.com/products",
                extract_items_func=lambda soup: web_scraper.extract_items(
                    soup,
                    container_selector=".product-item",
                    item_selectors={
                        "name": "h2.product-name",
                        "price": "span.product-price"
                    }
                ),
                next_page_selector="a.next-page",
                max_pages=3
            )
            
            # Save the scraped data
            web_scraper.save_data(products, "products", format="json")
            
            logger.info(f"Product scraping completed, found {len(products)} products")
            return len(products)
        
        except Exception as e:
            logger.error(f"Product scraping failed: {e}")
            raise
    
    def scrape_news():
        """Example task to scrape news articles."""
        logger.info("Starting news scraping task")
        web_scraper = scraper.WebScraper(rate_limit=1.5)
        
        try:
            news_articles = web_scraper.paginate(
                start_url="https://example.com/news",
                extract_items_func=lambda soup: web_scraper.extract_items(
                    soup,
                    container_selector=".news-article",
                    item_selectors={
                        "title": "h3.article-title",
                        "summary": "p.article-summary",
                        "date": "span.article-date"
                    }
                ),
                next_page_selector="a.next-page",
                max_pages=2
            )
            
            # Save the scraped data
            web_scraper.save_data(news_articles, "news_articles", format="json")
            
            logger.info(f"News scraping completed, found {len(news_articles)} articles")
            return len(news_articles)
        
        except Exception as e:
            logger.error(f"News scraping failed: {e}")
            raise
    
    # Register tasks with the scheduler
    scheduler.register_task(
        name="scrape_products_daily",
        function=scrape_products,
        interval=86400,  # Run daily (24 hours)
        retry_on_failure=True,
        max_retries=3
    )
    
    scheduler.register_task(
        name="scrape_news_hourly",
        function=scrape_news,
        interval=3600,  # Run hourly
        retry_on_failure=True,
        max_retries=2
    )
    
    # Example of a task with cron scheduling (runs at 2 AM every day)
    scheduler.register_task(
        name="scrape_daily_deals",
        function=scrape_products,
        kwargs={"deals_only": True},  # Example of passing custom parameters
        cron_expression="0 2 * * *"  # Run at 2:00 AM daily
    )
    
    # Start the scheduler
    scheduler.start()
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(60)
            
            # Print task status every minute
            task_status = scheduler.get_all_task_status()
            print("\nCurrent task status:")
            for name, status in task_status.items():
                print(f"- {name}: Next run at {status['next_run']}, Last status: {status['last_status']}")
    
    except KeyboardInterrupt:
        print("Shutting down...")
        scheduler.shutdown()