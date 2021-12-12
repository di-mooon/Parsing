import asyncio
import json
import os
import re
import logging
import requests
from aiohttp import ClientSession
from bs4 import BeautifulSoup

file_log = logging.FileHandler('Log.log')
console_out = logging.StreamHandler()

logging.basicConfig(
    handlers=(file_log, console_out),
    format='[%(asctime)s] [%(levelname)s]: %(message)s',
    datefmt='%m.%d.%Y %H:%M:%S'
)


class EldoradoParser:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.home_url = 'https://www.eldorado.ru'
        self.category_urls = f'{self.home_url}/d'
        self.headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
        }
        self.url_categories = self.get_url_categories()
        self.comments = []

    def get_url_categories(self):
        response = requests.get(url=self.category_urls, headers=self.headers)
        soup = BeautifulSoup(response.text, 'lxml')
        links = soup.find_all('a', {'class': 'ss'})
        urls = set()
        for link in links:
            url = re.search(r'/c/([\w.-]+)/', link.get('href'))
            if url:
                urls.add(url.group(0))
        return urls

    async def _parse_comments(self, session, url, page):
        params = {'show': 'response'}
        if page == 1:
            url = f'{url}'
        else:
            url = f'{url}/page/{page}'
        await asyncio.sleep(0.1)
        async with session.get(url=url, params=params, headers=self.headers) as res:
            response = await res.text()
            soup = BeautifulSoup(response, 'lxml')
            comments = soup.find_all('div', class_='usersReviewsListItemInnerContainer')
            for comment in comments:
                try:
                    user_info = {
                        "url": url,
                        "author": comment.find('span', class_='userName').text,
                        "date": comment.find('div', class_='userReviewDate').text.strip(),
                        "content": comment.find('div', class_='middleBlockItem').text.strip(),
                    }
                    self.comments.append(user_info)
                except Exception:
                    continue

    async def _queue_parse_comments(self, url, path_dir):
        params = {'show': 'response'}
        url = f'{self.home_url}/{url}'
        async with ClientSession(trust_env=True, headers=self.headers) as session:
            response = await session.get(url=url, headers=self.headers, params=params)
            if response.status == 200:
                soup = BeautifulSoup(await response.text(), 'lxml')
                try:
                    count_pages = int(soup.find('div', class_='pages').find_all('a')[-1].text)
                except (AttributeError, IndexError):
                    count_pages = 1
                product_name = soup.find('h1', class_='catalogItemDetailHd').text.replace('/', '_')
                tasks = []
                for page in range(count_pages):
                    task = asyncio.create_task(self._parse_comments(session, url, page + 1))
                    tasks.append(task)
                await asyncio.gather(*tasks)
                if self.comments:
                    with open(f"{path_dir}/{product_name}.json", 'w', encoding='utf-8') as file:
                        json.dump(self.comments, file, ensure_ascii=False, indent=4)
                    self.comments = []
            else:
                self.logger.warning(f'Error parse {url}')

    async def _parse_category(self, session, url, page, path_dir):
        await asyncio.sleep(0.1)
        async with session.get(url=url, params={'page': page}, headers=self.headers) as res:
            response = await res.text()
            soup = BeautifulSoup(response, 'lxml')
            links = soup.find_all('a', class_='sG')
            for a in links:
                await asyncio.sleep(1)
                await self._queue_parse_comments(a.get('href'), path_dir)

    async def _queue_parse_category(self, url):
        async with ClientSession(trust_env=True) as session:
            url = f'{self.home_url}{url}'
            response = await session.get(url=url, headers=self.headers)
            if response.status == 200:
                soup = BeautifulSoup(await response.text(), 'lxml')
                count_page = int(soup.find('div', class_='zr').find_all('li')[-2].text)
                category_name = soup.find('h1', class_='-K').text
                path_dir = f'reviews/{category_name}'
                os.makedirs(path_dir)
                tasks = []
                for page in range(count_page):
                    task = asyncio.create_task(self._parse_category(session, url, page + 1, path_dir))
                    tasks.append(task)
                await asyncio.gather(*tasks)
            else:
                self.logger.warning(f'Error parse {url}')

    def run_parse(self):
        for url_category in list(self.url_categories)[3:]:
            self.logger.info(f'Run parse {url_category}')
            asyncio.run(self._queue_parse_category(url_category))
            self.logger.info(f'End parse {url_category}')


parser = EldoradoParser()
parser.run_parse()
