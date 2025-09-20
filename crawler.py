import os
import psycopg2
import requests
import json
import time
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GitHubCrawler:
    def __init__(self):
        self.token = os.environ.get('GITHUB_TOKEN')
        self.db_url = os.environ.get('DATABASE_URL')
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
        }
        
    def connect_db(self):
        return psycopg2.connect(self.db_url)
    
    def fetch_repositories(self, cursor_after=None, limit=100):
        query = """
        query($cursor: String, $limit: Int!) {
          search(query: "stars:>1", type: REPOSITORY, first: $limit, after: $cursor) {
            edges {
              node {
                ... on Repository {
                  id
                  databaseId
                  name
                  nameWithOwner
                  owner {
                    login
                  }
                  stargazerCount
                  createdAt
                  updatedAt
                }
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
          rateLimit {
            remaining
            resetAt
          }
        }
        """
        
        variables = {
            "cursor": cursor_after,
            "limit": limit
        }
        
        response = requests.post(
            'https://api.github.com/graphql',
            headers=self.headers,
            json={'query': query, 'variables': variables}
        )
        
        return response.json()
    
    def save_repositories(self, repositories):
        conn = self.connect_db()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO repositories (id, name, full_name, owner_login, star_count, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) 
        DO UPDATE SET 
            star_count = EXCLUDED.star_count,
            updated_at = EXCLUDED.updated_at,
            crawled_at = CURRENT_TIMESTAMP;
        """
        
        for repo in repositories:
            cursor.execute(insert_query, (
                repo['databaseId'],
                repo['name'],
                repo['nameWithOwner'],
                repo['owner']['login'],
                repo['stargazerCount'],
                repo['createdAt'],
                repo['updatedAt']
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
    
    def crawl(self, target_count=100000):  # Start with 100000 for testing
        logger.info(f"Starting crawl for {target_count} repositories")
        
        cursor_after = None
        total_crawled = 0
        
        while total_crawled < target_count:
            try:
                response = self.fetch_repositories(cursor_after)
                
                if 'errors' in response:
                    logger.error(f"GraphQL errors: {response['errors']}")
                    break
                
                data = response['data']
                repositories = [edge['node'] for edge in data['search']['edges']]
                
                if not repositories:
                    logger.info("No more repositories found")
                    break
                
                self.save_repositories(repositories)
                total_crawled += len(repositories)
                
                logger.info(f"Crawled {total_crawled}/{target_count} repositories")
                
                # Check rate limit
                rate_limit = data['rateLimit']
                if rate_limit['remaining'] < 10:
                    reset_time = datetime.fromisoformat(rate_limit['resetAt'].replace('Z', '+00:00'))
                    wait_time = (reset_time - datetime.now()).total_seconds() + 60
                    logger.info(f"Rate limit low, waiting {wait_time} seconds")
                    time.sleep(wait_time)
                
                # Get next cursor
                page_info = data['search']['pageInfo']
                if page_info['hasNextPage']:
                    cursor_after = page_info['endCursor']
                else:
                    break
                    
                # Small delay between requests
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error during crawl: {e}")
                time.sleep(5)
        
        logger.info(f"Crawl completed. Total repositories: {total_crawled}")

if __name__ == "__main__":
    crawler = GitHubCrawler()
    crawler.crawl(100000)  # Start with 100000 repos
