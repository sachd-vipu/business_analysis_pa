import os
import json
from collections import defaultdict
from urllib.parse import urlparse
import networkx as nx
from pyvis.network import Network
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedVisualizer:
    def __init__(self):
        self.data_dir = 'crawled_data'
        self.output_dir = 'enhanced_visualizations'
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        # Define excluded domains and patterns
        self.excluded_patterns = [
            'facebook.com', 'instagram.com', 'twitter.com', 'x.com',
            'linkedin.com', 'youtube.com', 'shopify.com', 'mailto:',
            'tel:', 'phone', 'whatsapp', 'pinterest', 'tiktok',
            'google.com', 'apple.com', 'android', 'play.google.com',
            'bing.com',  'yahoo.com', 'goo.gl', 'bit.ly', 'tinyurl.com',
            'maps.google.com', 'maps.apple.com', 'maps.microsoft.com', 'maps.yandex.com',
            'mspd.app.goo.gl','javascript:void(0)', 'javascript:void(0);', 'maps.google.co.us',
            'cloudfare.org','creators.yahoo.com', 'help.yahoo.com', 'hyodrogen.shopify.dev',
            'legal.yahoo.com','lnkd.in','sopifystatus.com','shopifyinvestors.com','shopify.dev',
            'yelp.com','yahooinc.com','wp.me','wordpress.com','wordpress.org','vimeo.com',
            'yahoo.uservoice.com','ycombinator.com','ycombinator.net','ycombinator.org',
            'amazonaws.com','amazon.com','javascript:','javascript:;','javascript'

        ]
        
        # Store all domains and their company associations
        self.domain_to_companies = defaultdict(set)
        # Store cross-company connections
        self.cross_connections = defaultdict(set)
        
    def is_excluded_domain(self, url):
        """Check if the URL should be excluded based on patterns."""
        url_lower = url.lower()
        return any(pattern in url_lower for pattern in self.excluded_patterns)
        
    def extract_domain(self, url):
        """Extract clean domain from URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # Remove www. prefix if present
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return None
            
    def process_company_data(self, company_name):
        """Process a single company's graph data and build cross-connections."""
        company_dir = os.path.join(self.data_dir, company_name)
        graph_file = os.path.join(company_dir, 'graph_data.json')
        
        if not os.path.exists(graph_file):
            return None
            
        with open(graph_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # Process nodes and their connections
        for node in data['nodes']:
            source_url = node['id']
            source_domain = self.extract_domain(source_url)
            
            if not source_domain or self.is_excluded_domain(source_url):
                continue
                
            # Associate domain with company
            self.domain_to_companies[source_domain].add(company_name)
            
            # Process outgoing links
            for edge in data['edges']:
                if edge['from'] == source_url:
                    target_url = edge['to']
                    target_domain = self.extract_domain(target_url)
                    
                    if not target_domain or self.is_excluded_domain(target_url):
                        continue
                        
                    # Store the connection
                    self.cross_connections[source_domain].add(target_domain)
        
    def create_cross_company_visualization(self):
        """Create visualization showing connections between companies through shared domains."""
        # Create network for cross-company connections
        net = Network(height="900px", width="100%", bgcolor="#ffffff", font_color="black")
        net.force_atlas_2based()
        
        # Track added nodes to avoid duplicates
        added_nodes = set()
        
        # Add nodes and edges
        for source_domain, target_domains in self.cross_connections.items():
            source_companies = self.domain_to_companies[source_domain]
            
            # Add source node if not already added
            if source_domain not in added_nodes:
                companies_str = ", ".join(source_companies)
                node_size = 20 + (len(source_companies) * 5)  # Bigger node for domains shared by more companies
                net.add_node(source_domain, 
                            title=f"Domain: {source_domain}\nCompanies: {companies_str}",
                            size=node_size,
                            color='#1f77b4' if len(source_companies) > 1 else '#7f7f7f')
                added_nodes.add(source_domain)
            
            # Add edges to connected domains
            for target_domain in target_domains:
                target_companies = self.domain_to_companies[target_domain]
                
                # Skip if target is not associated with any company
                if not target_companies:
                    continue
                    
                # Add target node if not already added
                if target_domain not in added_nodes:
                    companies_str = ", ".join(target_companies)
                    node_size = 20 + (len(target_companies) * 5)
                    net.add_node(target_domain, 
                                title=f"Domain: {target_domain}\nCompanies: {companies_str}",
                                size=node_size,
                                color='#1f77b4' if len(target_companies) > 1 else '#7f7f7f')
                    added_nodes.add(target_domain)
                
                # Add edge
                if source_domain != target_domain:
                    net.add_edge(source_domain, target_domain)
        
        # Save the visualization
        output_file = os.path.join(self.output_dir, 'cross_company_connections.html')
        net.save_graph(output_file)
        
        # Create wrapper HTML with additional information
        self.create_cross_company_wrapper(output_file)
        
        return len(added_nodes)
        
    def create_cross_company_wrapper(self, graph_file):
        """Create a wrapper HTML file with statistics and legend."""
        stats_html = '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Cross-Company Domain Connections</title>
            <style>
                body {{ 
                    margin: 0; 
                    padding: 0; 
                    font-family: Arial, sans-serif; 
                }}
                .container {{ 
                    display: flex; 
                    height: 100vh; 
                }}
                .sidebar {{
                    width: 300px;
                    background-color: #f8f9fa;
                    padding: 20px;
                    border-right: 1px solid #dee2e6;
                    overflow-y: auto;
                }}
                .graph-container {{
                    flex-grow: 1;
                    height: 100%;
                }}
                .stats-box {{
                    background-color: white;
                    border: 1px solid #dee2e6;
                    border-radius: 5px;
                    padding: 15px;
                    margin-bottom: 20px;
                }}
                .legend-item {{
                    display: flex;
                    align-items: center;
                    margin-bottom: 10px;
                }}
                .color-box {{
                    width: 20px;
                    height: 20px;
                    margin-right: 10px;
                    border-radius: 3px;
                }}
                iframe {{
                    width: 100%;
                    height: 100%;
                    border: none;
                }}
                h3 {{ 
                    margin-top: 0; 
                }}
                .shared-domains {{
                    margin-top: 20px;
                }}
                .domain-item {{
                    background-color: white;
                    border: 1px solid #dee2e6;
                    border-radius: 3px;
                    padding: 10px;
                    margin-bottom: 10px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="sidebar">
                    <div class="stats-box">
                        <h3>Statistics</h3>
                        <p>Total Domains: {total_domains}</p>
                        <p>Shared Domains: {shared_domains}</p>
                        <p>Total Connections: {total_connections}</p>
                    </div>
                    
                    <div class="stats-box">
                        <h3>Legend</h3>
                        <div class="legend-item">
                            <div class="color-box" style="background-color: #1f77b4"></div>
                            <span>Shared Domain (appears in multiple companies)</span>
                        </div>
                        <div class="legend-item">
                            <div class="color-box" style="background-color: #7f7f7f"></div>
                            <span>Single Company Domain</span>
                        </div>
                    </div>
                    
                    <div class="shared-domains">
                        <h3>Domains Shared by Multiple Companies:</h3>
                        {shared_domains_list}
                    </div>
                </div>
                <div class="graph-container">
                    <iframe src="{graph_filename}"></iframe>
                </div>
            </div>
        </body>
        </html>
        '''
        
        # Calculate statistics
        total_domains = len(self.domain_to_companies)
        shared_domains = sum(1 for companies in self.domain_to_companies.values() if len(companies) > 1)
        total_connections = sum(len(targets) for targets in self.cross_connections.values())
        
        # Generate shared domains list
        shared_domains_html = ""
        for domain, companies in sorted(self.domain_to_companies.items()):
            if len(companies) > 1:
                shared_domains_html += f'''
                <div class="domain-item">
                    <strong>{domain}</strong><br>
                    Companies: {', '.join(sorted(companies))}
                </div>
                '''
        
        # Fill in the template
        html_content = stats_html.format(
            total_domains=total_domains,
            shared_domains=shared_domains,
            total_connections=total_connections,
            shared_domains_list=shared_domains_html,
            graph_filename=os.path.basename(graph_file)
        )
        
        # Save the wrapper file
        wrapper_path = os.path.join(self.output_dir, 'cross_company_analysis.html')
        with open(wrapper_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
    def process_all_companies(self):
        """Process all company data and generate visualizations."""
        # First pass: collect all domains and their relationships
        for company_dir in os.listdir(self.data_dir):
            full_path = os.path.join(self.data_dir, company_dir)
            if not os.path.isdir(full_path):
                continue
                
            logger.info(f"Processing {company_dir}...")
            self.process_company_data(company_dir)
        
        # Create cross-company visualization
        num_nodes = self.create_cross_company_visualization()
        logger.info(f"Created cross-company visualization with {num_nodes} domains")

def main():
    visualizer = EnhancedVisualizer()
    visualizer.process_all_companies()

if __name__ == "__main__":
    main() 