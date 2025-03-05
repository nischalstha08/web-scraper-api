"""
scheduler.py

This module sets up a background scheduler using APScheduler to periodically execute
web scraping tasks defined in scraper.py. It is designed to:
  - Run a static scraping job every 5 minutes.
  - Run a dynamic scraping job every 10 minutes.
  - Log detailed information about each job's execution.
  - Handle exceptions to ensure that any failures are logged for debugging.

Requirements:
  - APScheduler for scheduling.
  - A proper logging configuration to trace job execution.
  - The scraper.py module must contain the functions: scrape_static_data() and scrape_dynamic_data().

Usage:
  Run this file as a standalone script to start the scheduler. The scheduler will continue
  running in the background until you stop the process (e.g., by pressing Ctrl+C).

Author: [Your Name]
Date: [Current Date]
"""

import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Import the scraping functions from scraper.py
from scraper import scrape_static_data, scrape_dynamic_data

# Configure the logger to include the time, log level, and message.
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def job_static_scraping():
    """
    Executes the static scraping task.
    
    Calls the scrape_static_data() function, which should scrape a static website,
    store the resulting data (e.g., to a CSV file), and return the data as a DataFrame.
    
    Logs detailed information before, after, and in the case of errors.
    """
    logger.info("Starting static scraping job.")
    try:
        # Call the function that handles static scraping.
        data = scrape_static_data()
        if data is not None:
            logger.info("Static scraping completed successfully. Data saved to 'static_quotes.csv'.")
        else:
            logger.error("Static scraping completed, but no data was returned.")
    except Exception as e:
        logger.exception(f"Exception occurred during static scraping: {e}")


def job_dynamic_scraping():
    """
    Executes the dynamic scraping task.
    
    Calls the scrape_dynamic_data() function, which should scrape a JavaScript-rendered page
    (using Selenium), store the resulting data (e.g., to a CSV file), and return the data as a DataFrame.
    
    Logs detailed information about the process and handles any exceptions.
    """
    logger.info("Starting dynamic scraping job.")
    try:
        # Call the function that handles dynamic scraping.
        data = scrape_dynamic_data()
        if data is not None:
            logger.info("Dynamic scraping completed successfully. Data saved to 'dynamic_quotes.csv'.")
        else:
            logger.error("Dynamic scraping completed, but no data was returned.")
    except Exception as e:
        logger.exception(f"Exception occurred during dynamic scraping: {e}")


def main():
    """
    Configures and starts the APScheduler BackgroundScheduler.
    
    Schedules:
      - The static scraping job to run every 5 minutes.
      - The dynamic scraping job to run every 10 minutes.
    
    Keeps the scheduler running indefinitely until an exit signal (e.g., KeyboardInterrupt) is received.
    """
    # Initialize the BackgroundScheduler.
    scheduler = BackgroundScheduler()

    try:
        # Schedule the static scraping job every 5 minutes.
        scheduler.add_job(
            job_static_scraping,
            trigger=IntervalTrigger(minutes=5),
            id='static_scraping_job',
            replace_existing=True
        )
        logger.info("Scheduled static scraping job every 5 minutes.")

        # Schedule the dynamic scraping job every 10 minutes.
        scheduler.add_job(
            job_dynamic_scraping,
            trigger=IntervalTrigger(minutes=10),
            id='dynamic_scraping_job',
            replace_existing=True
        )
        logger.info("Scheduled dynamic scraping job every 10 minutes.")

        # Start the scheduler.
        scheduler.start()
        logger.info("Scheduler started successfully. Press Ctrl+C to exit.")

        # Keep the main thread alive to allow background jobs to run.
        while True:
            time.sleep(60)  # Sleep for 60 seconds
    except (KeyboardInterrupt, SystemExit):
        logger.info("Exit signal received. Shutting down scheduler...")
    except Exception as e:
        logger.exception(f"An error occurred while setting up the scheduler: {e}")
    finally:
        scheduler.shutdown()
        logger.info("Scheduler shut down successfully.")


if __name__ == '__main__':
    main()

