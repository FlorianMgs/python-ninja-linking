import csv
import os
import scrapy
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse


load_dotenv()


class DofollowSpider(scrapy.Spider):
    name = 'dofollow_spider'

    def __init__(self, keywords='', num_results=100, *args, **kwargs):
        super(DofollowSpider, self).__init__(*args, **kwargs)
        self.keywords = keywords.split(',')
        self.num_results = int(num_results)
        self.output_file = "dofollow_links.csv"
        self.api_key = os.getenv("GOOGLE_API_KEY")
        self.cse_id = os.getenv("CUSTOM_SEARCH_ENGINE_ID")
        self.csv_writer = None
        if not os.path.exists(self.output_file):
            with open(self.output_file, "w", newline='', encoding='utf-8') as f:
                self.csv_writer = csv.writer(f)
                self.csv_writer.writerow(['URL', 'DoFollow Link'])

    def closed(self, reason):
        if self.csv_writer:
            self.csv_writer.close()

    def start_requests(self):
        for keyword in self.keywords:
            urls = self.google_search(keyword)

            for url in urls:
                yield scrapy.Request(url=url, callback=self.parse)

    def google_search(self, query):
        service = build("customsearch", "v1", developerKey=self.api_key)
        urls = []

        max_results = min(100, self.num_results)  # Limit the maximum number of search results to 100
        for page in range(1, (max_results // 10) + 1):
            start_index = (page - 1) * 10 + 1
            results = service.cse().list(q=query, cx=self.cse_id, start=start_index).execute()

            for result in results.get('items', []):
                urls.append(result['link'])

        return urls

    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')

        # Check if the website allows posting or commenting
        form_keywords = ['comment', 'message', 'post', 'reply', 'discussion', 'topic', 'feedback', 'respond']
        forms = [
            form for form in soup.find_all('form') if 
            any((keyword in form.get('id', '').lower() or keyword in form.get('class', '')) 
            for keyword in form_keywords)
        ]

        if forms:
            # Search for DoFollow links in comments/messages posted by other users
            comments_container_keywords = ['comments', 'messages', 'posts', 'replies', 'discussions', 'topics', 'responses']
            comments_containers = [
                container for container in soup.find_all() if 
                any((keyword in container.get('id', '').lower() or keyword in container.get('class', '')) 
                for keyword in comments_container_keywords)
            ]

            for container in comments_containers:
                for a_tag in container.find_all('a', href=True):
                    if 'nofollow' not in a_tag.get('rel', []):
                        self.logger.info(f"Found a DoFollow link on {response.url}: {a_tag['href']}")
                        with open(self.output_file, "a", newline='', encoding='utf-8') as f:
                            self.csv_writer = csv.writer(f)
                            self.csv_writer.writerow([response.url, a_tag['href']])

def main():
    keywords = input("Enter the keywords to search for (separated by commas): ")
    num_results = int(input("Enter the number of desired search results per keyword: "))

    process = CrawlerProcess()
    process.crawl(DofollowSpider, keywords=keywords, num_results=num_results)
    process.start()


if __name__ == "__main__":
    main()
