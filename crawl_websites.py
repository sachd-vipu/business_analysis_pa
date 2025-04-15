import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import networkx as nx
from pyvis.network import Network
import time
from datetime import datetime
import html
import logging
import os
import json
from collections import deque

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebsiteCrawler:
    def __init__(self, max_depth=5, max_pages_per_site=100, delay=1):
        self.max_depth = max_depth
        self.max_pages_per_site = max_pages_per_site
        self.delay = delay
        self.graph = nx.DiGraph()
        self.visited = set()
        self.successful_crawls = set()
        self.failed_crawls = set()
        
        # Create directory for storing crawled data
        self.data_dir = 'crawled_data'
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
        
    def is_valid_url(self, url):
        """Check if URL is valid."""
        try:
            url = self.clean_url(url)
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False
            
    def clean_url(self, url):
        """Clean and normalize URL."""
        if not url:
            return ""
        url = url.strip().lower()
        if url.startswith('www.'):
            url = url[4:]
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url.rstrip('/')
        
    def get_domain(self, url):
        """Extract domain from URL."""
        return urlparse(url).netloc
        
    def save_crawled_data(self, company_name, url, graph_data):
        """Save crawled data for a successful website."""
        safe_filename = "".join(c for c in company_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_filename = safe_filename.replace(' ', '_')
        
        company_dir = os.path.join(self.data_dir, safe_filename)
        if not os.path.exists(company_dir):
            os.makedirs(company_dir)
            
        # Save graph data as JSON
        graph_data['company_name'] = company_name
        graph_data['start_url'] = url
        graph_data['crawl_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        json_file = os.path.join(company_dir, 'graph_data.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(graph_data, f, indent=2)
            
        # Create interactive visualization
        net = Network(height="750px", width="100%", bgcolor="#ffffff", font_color="black")
        
        # Add nodes
        for node in graph_data['nodes']:
            net.add_node(node['id'], 
                        label=node['label'],
                        title=node['title'],
                        color=node['color'])
        
        # Add edges
        for edge in graph_data['edges']:
            net.add_edge(edge['from'], 
                        edge['to'],
                        title=edge['title'])
        
        # Save the interactive visualization
        html_file = os.path.join(company_dir, 'graph.html')
        net.save_graph(html_file)
        
        logger.info(f"Saved crawled data for {company_name} to {company_dir}")
        
    def crawl(self, start_url, company_name):
        """Crawl a website using BFS and build the link graph."""
        if not start_url or not isinstance(start_url, str):
            logger.warning(f"Empty or invalid URL for {company_name}")
            self.failed_crawls.add((company_name, start_url, "Empty or invalid URL"))
            return False
            
        start_url = self.clean_url(start_url)
        if not self.is_valid_url(start_url):
            logger.warning(f"Invalid URL for {company_name}: {start_url}")
            self.failed_crawls.add((company_name, start_url, "Invalid URL"))
            return False
            
        try:
            logger.info(f"Starting crawl for {company_name} at {start_url}")
            
            # Initialize BFS queue with (url, depth)
            queue = deque([(start_url, 0)])
            visited = {start_url}
            
            # Initialize graph data structure
            graph_data = {
                'nodes': [{'id': start_url, 
                          'label': self.get_domain(start_url),
                          'title': start_url,
                          'color': '#97c2fc'}],  # Blue for start node
                'edges': []
            }
            
            while queue and len(visited) < self.max_pages_per_site:
                current_url, depth = queue.popleft()
                
                if depth >= self.max_depth:
                    continue
                    
                try:
                    response = requests.get(current_url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
                    if response.status_code != 200:
                        continue
                        
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Find all links
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        full_url = urljoin(current_url, href)
                        full_url = self.clean_url(full_url)
                        
                        if full_url not in visited and self.is_valid_url(full_url):
                            visited.add(full_url)
                            queue.append((full_url, depth + 1))
                            
                            # Add node and edge to graph data
                            graph_data['nodes'].append({
                                'id': full_url,
                                'label': self.get_domain(full_url),
                                'title': full_url,
                                'color': '#ffa07a' if self.get_domain(full_url) != self.get_domain(start_url) else '#97c2fc'
                            })
                            
                            graph_data['edges'].append({
                                'from': current_url,
                                'to': full_url,
                                'title': f"{current_url} → {full_url}"
                            })
                    
                    time.sleep(self.delay)  # Be nice to servers
                    
                except Exception as e:
                    logger.error(f"Error processing {current_url}: {str(e)}")
                    continue
            
            if len(visited) > 0:
                self.successful_crawls.add((company_name, start_url))
                logger.info(f"Successfully crawled {company_name}")
                self.save_crawled_data(company_name, start_url, graph_data)
                return True
            else:
                self.failed_crawls.add((company_name, start_url, "No pages crawled"))
                logger.warning(f"No pages crawled for {company_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error crawling {start_url}: {str(e)}")
            self.failed_crawls.add((company_name, start_url, str(e)))
            return False
            
    def generate_html_report(self):
        """Generate HTML report of successful crawls."""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Website Crawl Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .company {{ margin-bottom: 20px; padding: 10px; border: 1px solid #ccc; }}
                .success {{ color: green; }}
                .error {{ color: red; }}
                .graph {{ margin-top: 10px; }}
                .stats {{ background-color: #f5f5f5; padding: 10px; margin-bottom: 20px; }}
                .failed {{ color: red; }}
                .graph-container {{ width: 100%; height: 600px; border: 1px solid #ccc; margin-top: 10px; }}
            </style>
        </head>
        <body>
            <h1>Website Crawl Report</h1>
            <p>Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
            <div class="stats">
                <h2>Statistics</h2>
                <p>Total Companies Processed: {len(self.successful_crawls) + len(self.failed_crawls)}</p>
                <p>Successfully Crawled: {len(self.successful_crawls)}</p>
                <p>Failed Crawls: {len(self.failed_crawls)}</p>
            </div>
        """
        
        if self.successful_crawls:
            html_content += "<h2>Successful Crawls</h2>"
            for company_name, url in sorted(self.successful_crawls):
                safe_filename = company_name.replace(' ', '_')
                html_content += f"""
                <div class="company">
                    <h3>{html.escape(company_name)}</h3>
                    <p>URL: <a href="{html.escape(url)}">{html.escape(url)}</a></p>
                    <p>Data saved in: crawled_data/{html.escape(safe_filename)}/</p>
                    <div class="graph">
                        <h4>Interactive Link Graph:</h4>
                        <iframe src="crawled_data/{html.escape(safe_filename)}/graph.html" 
                                class="graph-container" 
                                frameborder="0">
                        </iframe>
                    </div>
                </div>
                """
        
        if self.failed_crawls:
            html_content += "<h2>Failed Crawls</h2>"
            for company_name, url, error in sorted(self.failed_crawls):
                html_content += f"""
                <div class="company failed">
                    <h3>{html.escape(company_name)}</h3>
                    <p>URL: <a href="{html.escape(url)}">{html.escape(url)}</a></p>
                    <p>Error: {html.escape(error)}</p>
                </div>
                """
            
        html_content += """
        </body>
        </html>
        """
        
        with open('crawl_report.html', 'w', encoding='utf-8') as f:
            f.write(html_content)

def main():
    try:
        # Read the CSV file
        logger.info("Reading CSV file...")
        df = pd.read_csv('All_data.csv')
        
        # Initialize crawler
        crawler = WebsiteCrawler(max_depth=5, max_pages_per_site=100, delay=2)
        
        # Process each company
        total = len(df)
        successful = 0
        
        for idx, row in df.iterrows():
            company_name = row['Company Name']
            url = row['Web Address (URL)']
            
            if pd.notna(url) and isinstance(url, str):
                logger.info(f"Processing {idx + 1}/{total}: {company_name}")
                if crawler.crawl(url, company_name):
                    successful += 1
                    
        logger.info(f"Crawling completed. Successfully crawled {successful} out of {total} websites.")
        
        # Generate HTML report
        crawler.generate_html_report()
        logger.info("HTML report generated as 'crawl_report.html'")
        
    except Exception as e:
        logger.error(f"An error occurred in main: {str(e)}")
        raise

if __name__ == "__main__":
    main() 