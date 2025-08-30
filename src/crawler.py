import aiohttp
import asyncio
import time
import re
import hashlib
import os
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

class RobotsTxt:
    def __init__(self, domain):
        self.domain = domain
        self.rules = {}
        self.crawl_delay = 1
        self.last_access = {}
        self._fetch_robots()

    def _fetch_robots(self):
        url = f"{self.domain}/robots.txt"
        try:
            async def fetch():
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            text = await resp.text()
                            self._parse(text)
            asyncio.run(fetch())
        except Exception:
            pass

    def _parse(self, text):
        for line in text.splitlines():
            if line.lower().startswith('crawl-delay:'):
                self.crawl_delay = int(re.findall(r'\d+', line)[0])
            elif line.lower().startswith('disallow:'):
                path = line.split(':', 1)[1].strip()
                self.rules[path] = True

    def allowed(self, url):
        path = urlparse(url).path
        for rule in self.rules:
            if path.startswith(rule):
                return False
        return True

    def wait(self, url):
        domain = urlparse(url).netloc
        now = time.time()
        last = self.last_access.get(domain, 0)
        wait_time = max(0, self.crawl_delay - (now - last))
        if wait_time > 0:
            time.sleep(wait_time)
        self.last_access[domain] = time.time()

class Crawler:
    def __init__(self, storage_dir, max_concurrency=5):
        self.frontier = set()
        self.visited = set()
        self.storage_dir = storage_dir
        self.max_concurrency = max_concurrency
        self.robots = {}
        os.makedirs(storage_dir, exist_ok=True)

    async def fetch(self, url):
        domain = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        if domain not in self.robots:
            self.robots[domain] = RobotsTxt(domain)
        if not self.robots[domain].allowed(url):
            return None
        self.robots[domain].wait(url)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200 and 'text/html' in resp.headers.get('content-type', ''):
                        text = await resp.text()
                        return text
        except Exception:
            return None
        return None

    async def crawl(self, seeds, limit=1000):
        self.frontier.update(seeds)
        sem = asyncio.Semaphore(self.max_concurrency)
        async def worker():
            while self.frontier and len(self.visited) < limit:
                url = self.frontier.pop()
                if url in self.visited:
                    continue
                async with sem:
                    html = await self.fetch(url)
                    if html:
                        self.save(url, html)
                        self.visited.add(url)
                        for link in self.extract_links(url, html):
                            if link not in self.visited:
                                self.frontier.add(link)
        await asyncio.gather(*[worker() for _ in range(self.max_concurrency)])

    def save(self, url, html):
        h = hashlib.sha256(url.encode()).hexdigest()
        meta = {
            'url': url,
            'fetch_time': time.time(),
            'content_hash': hashlib.sha256(html.encode()).hexdigest()
        }
        with open(os.path.join(self.storage_dir, f'{h}.html'), 'w', encoding='utf-8') as f:
            f.write(html)
        with open(os.path.join(self.storage_dir, f'{h}.meta'), 'w', encoding='utf-8') as f:
            f.write(str(meta))

    def extract_links(self, base_url, html):
        soup = BeautifulSoup(html, 'lxml')
        links = set()
        for a in soup.find_all('a', href=True):
            href = a['href']
            if href.startswith('http'):
                links.add(href)
            elif href.startswith('/'):
                links.add(urljoin(base_url, href))
        return links
