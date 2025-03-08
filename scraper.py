"""
scraper.py - Web Scraping Module

A robust, efficient web scraper that can extract data from websites with proper
error handling, rate limiting, and data processing capabilities.
"""

import logging
import time
import random
from typing import Dict, List, Any, Optional, Union, Callable
import json
import os
from datetime import datetime
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("web_scraper")

class WebScraper:
    """A scalable web scraper with built-in retry, rate limiting and proxy support."""
    
    def __init__(self, 
                 base_url: str = None,
                 retry_attempts: int = 3,
                 backoff_factor: float = 0.3,
                 timeout: int = 10,
                 rate_limit: float = 1.0,
                 random_delay: bool = True,
                 use_proxies: bool = False,
                 proxy_list: List[str] = None,
                 headers: Dict[str, str] = None,
                 cookies: Dict[str, str] = None,
                 data_dir: str = "scraped_data"):
        """
        Initialize the WebScraper with customizable parameters.
        
        Args:
            base_url: Base URL for the website to scrape
            retry_attempts: Number of retry attempts for failed requests
            backoff_factor: Backoff factor for exponential delay between retries
            timeout: Request timeout in seconds
            rate_limit: Minimum time between requests in seconds
            random_delay: Whether to add random delay between requests
            use_proxies: Whether to use proxies for requests
            proxy_list: List of proxy URLs to rotate through
            headers: Custom headers for requests
            cookies: Custom cookies for requests
            data_dir: Directory to store scraped data
        """
        self.base_url = base_url
        self.retry_attempts = retry_attempts
        self.backoff_factor = backoff_factor
        self.timeout = timeout
        self.rate_limit = rate_limit
        self.random_delay = random_delay
        self.use_proxies = use_proxies
        self.proxy_list = proxy_list or []
        self.current_proxy_index = 0
        self.last_request_time = 0
        self.user_agent = UserAgent()
        self.session = self._create_session()
        
        # Default headers if none provided
        self.headers = headers or {
            'User-Agent': self.user_agent.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }
        
        self.cookies = cookies or {}
        
        # Ensure data directory exists
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
        
        logger.info(f"Initialized WebScraper for {base_url if base_url else 'multiple websites'}")
    
    def _create_session(self) -> requests.Session:
        """
        Create a requests session with retry capabilities.
        
        Returns:
            A configured requests Session object
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.retry_attempts,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _get_proxy(self) -> Optional[Dict[str, str]]:
        """
        Get the next proxy from the proxy list.
        
        Returns:
            Dict with proxy URL or None if proxies are disabled
        """
        if not self.use_proxies or not self.proxy_list:
            return None
        
        proxy_url = self.proxy_list[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        
        return {"http": proxy_url, "https": proxy_url}
    
    def _rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        if self.last_request_time == 0:
            self.last_request_time = time.time()
            return
        
        elapsed = time.time() - self.last_request_time
        delay = self.rate_limit
        
        # Add random delay if enabled
        if self.random_delay:
            delay += random.uniform(0, self.rate_limit * 0.5)
        
        if elapsed < delay:
            sleep_time = delay - elapsed
            logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def request(self, 
                url: str, 
                method: str = "GET", 
                params: Dict = None, 
                data: Dict = None,
                json_data: Dict = None,
                update_headers: Dict = None,
                update_cookies: Dict = None) -> requests.Response:
        """
        Make an HTTP request with rate limiting and error handling.
        
        Args:
            url: URL to request
            method: HTTP method (GET or POST)
            params: URL parameters
            data: Form data for POST requests
            json_data: JSON data for POST requests
            update_headers: Additional headers to update for this request
            update_cookies: Additional cookies to update for this request
            
        Returns:
            Response object if successful
            
        Raises:
            requests.exceptions.RequestException: If request fails after retries
        """
        # Apply rate limiting
        self._rate_limit()
        
        # Construct full URL if relative path is provided
        if self.base_url and not url.startswith(('http://', 'https://')):
            full_url = f"{self.base_url.rstrip('/')}/{url.lstrip('/')}"
        else:
            full_url = url
        
        # Prepare headers and cookies for this request
        request_headers = self.headers.copy()
        if update_headers:
            request_headers.update(update_headers)
        
        # Rotate user agent for each request
        request_headers['User-Agent'] = self.user_agent.random
        
        request_cookies = self.cookies.copy()
        if update_cookies:
            request_cookies.update(update_cookies)
        
        # Get proxy if enabled
        proxies = self._get_proxy()
        
        try:
            logger.info(f"Making {method} request to {full_url}")
            
            if method.upper() == "GET":
                response = self.session.get(
                    full_url,
                    params=params,
                    headers=request_headers,
                    cookies=request_cookies,
                    proxies=proxies,
                    timeout=self.timeout
                )
            elif method.upper() == "POST":
                response = self.session.post(
                    full_url,
                    params=params,
                    data=data,
                    json=json_data,
                    headers=request_headers,
                    cookies=request_cookies,
                    proxies=proxies,
                    timeout=self.timeout
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Check for HTTP errors
            response.raise_for_status()
            
            return response
        
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {e}")
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout error: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            raise
    
    def get_soup(self, 
                 url: str, 
                 params: Dict = None,
                 parser: str = "html.parser") -> BeautifulSoup:
        """
        Get BeautifulSoup object from URL.
        
        Args:
            url: URL to scrape
            params: URL parameters
            parser: BeautifulSoup parser to use
            
        Returns:
            BeautifulSoup object
        """
        response = self.request(url, params=params)
        return BeautifulSoup(response.text, parser)
    
    def extract_data(self, 
                     soup: BeautifulSoup, 
                     selectors: Dict[str, str],
                     extractor_functions: Dict[str, Callable] = None) -> Dict[str, Any]:
        """
        Extract data from BeautifulSoup object using CSS selectors.
        
        Args:
            soup: BeautifulSoup object
            selectors: Dict mapping field names to CSS selectors
            extractor_functions: Dict mapping field names to custom extractor functions
            
        Returns:
            Dict with extracted data
        """
        result = {}
        extractor_functions = extractor_functions or {}
        
        for field, selector in selectors.items():
            try:
                if field in extractor_functions:
                    # Use custom extractor function if provided
                    result[field] = extractor_functions[field](soup, selector)
                else:
                    # Default extraction: get text from first matching element
                    element = soup.select_one(selector)
                    result[field] = element.get_text(strip=True) if element else None
            except Exception as e:
                logger.warning(f"Error extracting {field} with selector {selector}: {e}")
                result[field] = None
        
        return result
    
    def extract_items(self, 
                      soup: BeautifulSoup, 
                      container_selector: str,
                      item_selectors: Dict[str, str],
                      extractor_functions: Dict[str, Callable] = None) -> List[Dict[str, Any]]:
        """
        Extract multiple items from a container element.
        
        Args:
            soup: BeautifulSoup object
            container_selector: CSS selector for container elements
            item_selectors: Dict mapping field names to CSS selectors relative to container
            extractor_functions: Dict mapping field names to custom extractor functions
            
        Returns:
            List of dicts with extracted data
        """
        items = []
        extractor_functions = extractor_functions or {}
        
        containers = soup.select(container_selector)
        logger.info(f"Found {len(containers)} items with selector: {container_selector}")
        
        for container in containers:
            item_data = {}
            for field, selector in item_selectors.items():
                try:
                    if field in extractor_functions:
                        # Use custom extractor function if provided
                        item_data[field] = extractor_functions[field](container, selector)
                    else:
                        # Default extraction: get text from first matching element
                        element = container.select_one(selector)
                        item_data[field] = element.get_text(strip=True) if element else None
                except Exception as e:
                    logger.warning(f"Error extracting {field} with selector {selector}: {e}")
                    item_data[field] = None
            
            items.append(item_data)
        
        return items
    
    def save_data(self, 
                  data: Union[Dict, List], 
                  filename: str, 
                  format: str = "json") -> str:
        """
        Save scraped data to a file.
        
        Args:
            data: Data to save (dict or list)
            filename: Base filename (without extension)
            format: File format (json or csv)
            
        Returns:
            Path to saved file
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if not filename.endswith(f"_{timestamp}"):
            filename = f"{filename}_{timestamp}"
        
        if format.lower() == "json":
            filepath = os.path.join(self.data_dir, f"{filename}.json")
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        
        elif format.lower() == "csv":
            filepath = os.path.join(self.data_dir, f"{filename}.csv")
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False, encoding='utf-8')
        
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Saved data to {filepath}")
        return filepath

    def paginate(self, 
                 start_url: str,
                 extract_items_func: Callable[[BeautifulSoup], List[Dict]],
                 next_page_selector: str = None,
                 next_page_extractor: Callable[[BeautifulSoup], Optional[str]] = None,
                 max_pages: int = None) -> List[Dict]:
        """
        Scrape multiple pages by following pagination links.
        
        Args:
            start_url: URL of the first page
            extract_items_func: Function that extracts items from a page
            next_page_selector: CSS selector for the "next page" link
            next_page_extractor: Custom function to extract next page URL
            max_pages: Maximum number of pages to scrape
            
        Returns:
            List of all extracted items
        """
        all_items = []
        current_url = start_url
        page_num = 1
        
        while current_url:
            logger.info(f"Scraping page {page_num}: {current_url}")
            
            # Get page content
            soup = self.get_soup(current_url)
            
            # Extract items from current page
            page_items = extract_items_func(soup)
            all_items.extend(page_items)
            
            logger.info(f"Extracted {len(page_items)} items from page {page_num}")
            
            # Check if we've reached the maximum number of pages
            if max_pages and page_num >= max_pages:
                logger.info(f"Reached maximum number of pages ({max_pages})")
                break
            
            # Get next page URL
            if next_page_extractor:
                next_url = next_page_extractor(soup)
            elif next_page_selector:
                next_link = soup.select_one(next_page_selector)
                next_url = next_link.get('href') if next_link else None
                
                # Handle relative URLs
                if next_url and not next_url.startswith(('http://', 'https://')):
                    parsed_url = urlparse(current_url)
                    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                    next_url = f"{base_url}/{next_url.lstrip('/')}"
            else:
                next_url = None
            
            # Break if there's no next page
            if not next_url or next_url == current_url:
                logger.info("No next page found")
                break
            
            current_url = next_url
            page_num += 1
            
            # Add a bit more delay between page requests to be extra respectful
            time.sleep(self.rate_limit * 1.5)
        
        return all_items

# Example usage
if __name__ == "__main__":
    # Example: Scraping a product listing
    scraper = WebScraper(
        rate_limit=2.0,  # 2 seconds between requests
        random_delay=True
    )
    
    try:
        # Example: Extract product details from a single page
        soup = scraper.get_soup("https://example.com/products/1")
        
        product = scraper.extract_data(
            soup,
            selectors={
                "name": "h1.product-title",
                "price": "span.price",
                "description": "div.product-description",
                "rating": "div.rating"
            }
        )
        
        print(f"Product: {product}")
        
        # Example: Extract multiple products from a listing page
        soup = scraper.get_soup("https://example.com/products")
        
        products = scraper.extract_items(
            soup,
            container_selector=".product-item",
            item_selectors={
                "name": "h2.product-name",
                "price": "span.product-price",
                "url": "a.product-link"
            },
            extractor_functions={
                "url": lambda container, selector: container.select_one(selector)["href"]
                                                  if container.select_one(selector) else None
            }
        )
        
        print(f"Found {len(products)} products")
        
        # Save data
        scraper.save_data(products, "products", format="json")
        
        # Example: Paginated scraping
        all_products = scraper.paginate(
            start_url="https://example.com/products",
            extract_items_func=lambda soup: scraper.extract_items(
                soup,
                container_selector=".product-item",
                item_selectors={
                    "name": "h2.product-name",
                    "price": "span.product-price"
                }
            ),
            next_page_selector="a.next-page",
            max_pages=5
        )
        
        print(f"Total products across all pages: {len(all_products)}")
        
    except Exception as e:
        logger.error(f"Scraping failed: {e}")