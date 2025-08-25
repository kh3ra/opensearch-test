import csv
import logging
import random
import threading
import time
import argparse
import re
import boto3
import pandas as pd
from requests_aws4auth import AWS4Auth
from aws_requests_auth.aws_auth import AWSRequestsAuth
from opensearchpy import OpenSearch, RequestsHttpConnection
from datetime import datetime
import json
from faker import Faker
import os

class DocumentGenerator:
    """Generate random documents of at least the specified size"""
    
    def __init__(self, min_size_kb):
        self.faker = Faker()
        self.min_size_bytes = min_size_kb * 1024
        self.logger = logging.getLogger(__name__)

    def generate_document(self, test_id):
        """Generate a random document meeting minimum size requirement"""
        doc = self._generate_random_doc()
        
        # Add metadata
        doc['metadata'] = {
            'test_id': test_id,
            'timestamp': datetime.now().isoformat()
        }
        
        # If document is smaller than minimum size, pad the description
        current_size = len(json.dumps(doc).encode('utf-8'))
        if current_size < self.min_size_bytes:
            padding_size = self.min_size_bytes - current_size + 100  # Add buffer for JSON overhead
            doc['description'] = self.faker.text(max_nb_chars=int(padding_size))
        
        final_size = len(json.dumps(doc).encode('utf-8'))
        self.logger.debug(f"Generated {doc['type']} document of size {final_size/1024:.2f}KB")
        
        return doc

    def _generate_random_doc(self):
        """Generate random document of different types"""
        doc_types = ['user', 'product', 'article', 'event']
        doc_type = random.choice(doc_types)

        if doc_type == 'user':
            doc = {
                'type': 'user',
                'name': self.faker.name(),
                'email': self.faker.email(),
                'age': random.randint(18, 80),
                'city': self.faker.city(),
                'country': self.faker.country(),
                'description': ''  # Will be filled if needed
            }
        elif doc_type == 'product':
            doc = {
                'type': 'product',
                'name': self.faker.catch_phrase(),
                'price': round(random.uniform(10, 1000), 2),
                'category': random.choice(['Electronics', 'Clothing', 'Books', 'Food']),
                'in_stock': random.choice([True, False]),
                'description': ''  # Will be filled if needed
            }
        elif doc_type == 'article':
            doc = {
                'type': 'article',
                'title': self.faker.sentence(),
                'author': self.faker.name(),
                'content': self.faker.text(),
                'tags': random.sample(['tech', 'news', 'sports', 'health', 'science'], 
                                   k=random.randint(1, 3)),
                'description': ''  # Will be filled if needed
            }
        else:  # event
            doc = {
                'type': 'event',
                'name': self.faker.sentence(),
                'date': self.faker.date().format(),
                'location': self.faker.address(),
                'attendees': random.randint(10, 1000),
                'description': ''  # Will be filled if needed
            }

        return doc



class Config:
    def __init__(self, args):
        # Parse URL and set connection settings
        self._parse_url(args.url)
        self.use_ssl = self.scheme == 'https'
        
        # Rest of the settings...
        self.auth_type = args.auth_type
        self.region = args.region
        self.access_key = args.access_key
        self.secret_key = args.secret_key
        self.username = args.username
        self.password = args.password
        
        self.indexes = args.indexes
        self.iterations = args.iterations
        self.timeout_ms = args.timeout
        self.doc_threshold = args.doc_threshold
        self.min_doc_size_kb = args.min_doc_size_kb
        self.poll_interval = args.poll_interval
        self.output_dir = f"results_{time.strftime('%Y%m%d_%H%M%S')}"
        
        self.validate()

    @classmethod
    def from_args(cls):
        """Create Config instance from command line arguments"""
        parser = cls._create_parser()
        args = parser.parse_args()
        return cls(args)


    def _parse_url(self, url):
        """Parse OpenSearch URL into components"""
        import urllib.parse
        parsed = urllib.parse.urlparse(url)
        
        self.scheme = parsed.scheme
        self.host = parsed.hostname
        self.port = parsed.port or (443 if parsed.scheme == 'https' else 9200)
        
        # Handle path if present
        self.path = parsed.path.strip('/') if parsed.path else None
        
        # Handle basic auth in URL if present
        if parsed.username and parsed.password:
            self.auth_type = 'basic'
            self.username = parsed.username
            self.password = parsed.password

    @staticmethod
    def _create_parser():
        parser = argparse.ArgumentParser(description='Test OpenSearch replication latency')
        
        # URL argument instead of separate host/port
        parser.add_argument(
            '--url',
            required=True,
            help='OpenSearch URL (e.g., https://localhost:9200 or https://user:pass@host:port)'
        )
        
        # Authentication arguments
        parser.add_argument(
            '--auth-type',
            choices=['none', 'basic', 'aws'],
            default='none',
            help='Authentication type (none, basic, or aws)'
        )
        parser.add_argument('--region', help='AWS region (required for AWS auth)')
        parser.add_argument('--access-key', help='AWS access key (required for AWS auth)')
        parser.add_argument('--secret-key', help='AWS secret key (required for AWS auth)')
        parser.add_argument('--username', help='Basic auth username (overrides URL credentials)')
        parser.add_argument('--password', help='Basic auth password (overrides URL credentials)')
        
        # Test configuration arguments
        parser.add_argument('--indexes', nargs='+', required=True, help='List of index names to test')
        parser.add_argument('--iterations', type=int, default=100, help='Number of iterations per index')
        parser.add_argument('--timeout', type=int, default=1000, help='Operation timeout in milliseconds')
        parser.add_argument('--doc-threshold', type=int, default=1000, 
                          help='Minimum number of documents required in index')
        parser.add_argument('--min-doc-size-kb', type=int, default=1, 
                          help='Minimum document size in KB')
        parser.add_argument('--poll-interval', type=int, default=60, 
                          help='Index document count polling interval in seconds')
        
        return parser

    def validate(self):
        """Validate configuration settings"""
        if not self.host:
            raise ValueError("Invalid URL: no host specified")
            
        if self.auth_type == 'aws':
            if not all([self.region, self.access_key, self.secret_key]):
                raise ValueError("AWS authentication requires region, access key, and secret key")
        
        if self.auth_type == 'basic' and not (self.username and self.password):
            raise ValueError("Basic authentication requires username and password")

    def create_opensearch_client(self):
        """Create OpenSearch client based on configuration"""
        hosts = [{'host': self.host, 'port': self.port}]
        
        if self.path:
            hosts[0]['url_prefix'] = self.path

        if self.auth_type == 'aws':
            auth = AWS4Auth(
                self.access_key,
                self.secret_key,
                self.region,
                'es'
            )
        elif self.auth_type == 'basic':
            auth = (self.username, self.password)
        else:
            auth = None

        return OpenSearch(
            hosts=hosts,
            use_ssl=self.use_ssl,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            http_auth=auth if auth else None
        )

    def __str__(self):
        """String representation of configuration (excluding sensitive data)"""
        return (
            f"Config:\n"
            f"  URL: {self.scheme}://{self.host}:{self.port}"
            f"{f'/{self.path}' if self.path else ''}\n"
            f"  Auth Type: {self.auth_type}\n"
            f"  Indexes: {', '.join(self.indexes)}\n"
            f"  Iterations: {self.iterations}\n"
            f"  Timeout: {self.timeout_ms}ms\n"
            f"  Doc Threshold: {self.doc_threshold}\n"
            f"  Min Doc Size: {self.min_doc_size_kb}KB\n"
            f"  Poll Interval: {self.poll_interval}s\n"
            f"  Output Directory: {self.output_dir}"
        )

        
class IndexMonitor:
    """Monitor index document count"""
    
    def __init__(self, client, config):
        self.client = client
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def wait_for_threshold(self, index):
        """Wait until index has required number of documents"""
        while True:
            try:
                count = self.get_doc_count(index)
                self.logger.info(f"Index {index} has {count} documents. Threshold: {self.config.doc_threshold}")
                
                if count >= self.config.doc_threshold:
                    return True
                
                time.sleep(self.config.poll_interval)
                
            except Exception as e:
                self.logger.error(f"Error checking document count for {index}: {e}")
                time.sleep(self.config.poll_interval)
    
    def get_doc_count(self, index):
        """Get document count for an index"""
        stats = self.client.count(index=index)
        return stats['count']

class TestResultsHandler:
    """Handle test results storage and reporting"""
    
    def __init__(self, output_dir):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def save_result(self, index, result):
        """Save individual test result to CSV"""
        if result is None:
            self.logger.warning("Attempted to save None result, skipping")
            return
            
        csv_file = os.path.join(self.output_dir, f"{index}_results.csv")
        file_exists = os.path.exists(csv_file)
        
        with open(csv_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'test_id', 'timestamp', 'doc_type', 'doc_size_kb',
                'indexing_time_ms', 'replication_time_ms', 'timeouts'
            ])
            if not file_exists:
                writer.writeheader()
            writer.writerow(result)
    
    def generate_report(self, results):
        """Generate final report"""
        report = {}
        self.logger.info(json.dumps(results, indent=5))
        for index, index_results in results.items():
            df = pd.DataFrame(index_results)
        
            report[index] = {
                'total_tests': len(df),
                'successful_tests': len(df.dropna()),
                'indexing_time_ms': {
                    'p50': df['indexing_time_ms'].quantile(0.5),
                    'p90': df['indexing_time_ms'].quantile(0.9),
                    'p99': df['indexing_time_ms'].quantile(0.99)
                },
                'replication_time_ms': {
                    'p50': df['replication_time_ms'].quantile(0.5),
                    'p90': df['replication_time_ms'].quantile(0.9),
                    'p99': df['replication_time_ms'].quantile(0.99)
                }
            }
        
        # Save report
        report_file = os.path.join(self.output_dir, 'report.txt')
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        return report

class ReplicationTester:
    """Handle replication testing logic"""
    
    def __init__(self, client, config):
        self.client = client
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.doc_gen = DocumentGenerator(config.min_doc_size_kb)
        self.results_handler = TestResultsHandler(config.output_dir)
        self.results = {index: [] for index in config.indexes}
        self.timeouts = dict([(index, {"primary": 0, "replica": 0}) for index in self.config.indexes])
    
    def run_tests(self):
        """Run replication tests for all configured indexes"""
        monitor = IndexMonitor(self.client, self.config)
        
        for index in self.config.indexes:
            self.logger.info(f"Waiting for index {index} to reach threshold...")
            if monitor.wait_for_threshold(index):
                self.logger.info(f"Starting tests for index {index}")
                self.test_index(index)
            else:
                self.logger.warning(f"Skipping tests for index {index}")
        
        return self.results_handler.generate_report(self.results)
    
    def test_index(self, index):
            """Run tests for a single index"""
            successful_iterations = 0
            iteration = 0
            
            while successful_iterations < self.config.iterations:
                if iteration % 10 == 0:
                    self.logger.info(
                        f"Completed {successful_iterations}/{self.config.iterations} iterations "
                        f"for {index} (Timeouts - Primary: {self.timeouts[index]['primary']}, "
                        f"Replica: {self.timeouts[index]['replica']})"
                    )
                
                result = self.run_single_test(index)
                if result is not None:  # Only save and count successful iterations
                    self.results[index].append(result)
                    self.results_handler.save_result(index, result)
                    successful_iterations += 1
                
                iteration += 1
                
                # Optional: Add a limit to total iterations to prevent infinite loops
                if iteration > self.config.iterations * 3:  # Allow 3x the requested iterations
                    self.logger.warning(
                        f"Stopping after {iteration} attempts with only {successful_iterations} "
                        f"successful iterations for {index}"
                    )
                    break
            
            self.logger.info(
                f"Finished testing {index}. "
                f"Total attempts: {iteration}, "
                f"Successful: {successful_iterations}, "
                f"Primary timeouts: {self.timeouts[index]['primary']}, "
                f"Replica timeouts: {self.timeouts[index]['replica']}"
            )
    
    def wait_for_document(self, index, doc_id, is_primary=True):
        """
        Wait for document to be searchable on primary or replica.
        Returns time taken in milliseconds or None if timeout occurs.
        """
        start_time = time.time()
        preference = '_primary' if is_primary else '_replica'
        node_type = 'primary' if is_primary else 'replica'
        res = False
        while not res:
            try:
                response = self.client.get(
                    index=index,
                    id=doc_id,
                    preference=preference,
                    timeout=self.config.timeout_ms
                )
                res = response['found']
                if res:
                    return (time.time() - start_time) * 1000

            except Exception as e:
                if 'timeout' in str(e).lower():
                    self.logger.warning(f"Request timed out {node_type}: {e}")
                    self.timeouts[index][node_type] += 1

        return None
                

    def run_single_test(self, index):
        """Run a single test iteration"""
        test_id = f"test_{time.time_ns()}"
        doc = self.doc_gen.generate_document(test_id)
        
        try:
            self.client.index(index=index, body=doc, id=test_id)
        except Exception as e:
            self.logger.error(f"Failed to index document: {e}")
            return None

        # First wait for document on primary
        primary_time = self.wait_for_document(index, test_id, is_primary=True)
        if primary_time is None:
            self.logger.info(f"Skipping iteration due to primary timeout: {test_id}")
            return None

        # Only check replica after document is available on primary
        replica_time = self.wait_for_document(index, test_id, is_primary=False)
        if replica_time is None:
            self.logger.info(f"Skipping iteration due to replica timeout: {test_id}")
            return None

        return {
            'test_id': test_id,
            'timestamp': datetime.now().isoformat(),
            'doc_type': doc['type'],
            'doc_size_kb': len(json.dumps(doc).encode('utf-8')) / 1024,
            'indexing_time_ms': primary_time,
            'replication_time_ms': replica_time - primary_time,
            'timeouts': self.timeouts[index]
        }
        

def main():
    # Initialize logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logging.getLogger('opensearch').propagate = False

    try:
        # Initialize configuration
        config = Config.from_args()
        logger.info(f"Starting tests with configuration:\n{config}")

        # Create OpenSearch client
        client = config.create_opensearch_client()

        # Initialize and run tests
        tester = ReplicationTester(client, config)
        results = tester.run_tests()

        # Print results
        logger.info("Test Results:")
        print(json.dumps(results, indent=2))
        logger.info(f"Detailed results saved in: {config.output_dir}")

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        exit(1)

if __name__ == "__main__":
    main()
