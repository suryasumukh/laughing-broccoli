import requests
import threading
import queue
import time

from ratelimit import limits, sleep_and_retry
from collections import Counter, deque

from typing import Dict, Any, Tuple

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class PLOSAPIReader(object):
    def __init__(self):
        self.url = "http://api.plos.org/search?q=tobacco&start={}&rows=100"
        self._start = 0

    @sleep_and_retry
    @limits(calls=300, period=60 * 60)
    @limits(calls=10, period=60)
    def __next__(self):
        logger.debug('Fetching PLOS articles starting from {}'.format(self._start))
        response = requests.get(url=self.url.format(self._start))
        if response.status_code == 200:
            self._start += 100
            content = response.json().get('response', None)
            if not content:
                logger.info('No content from PLOS. Stopping iteration')
                raise StopIteration()
            docs = content.get('docs', None)
            if not docs:
                logger.info('No more articles for `tobacco` from PLOS. Stopping iteration')
                raise StopIteration()
            return docs
        else:
            return []

    def __iter__(self):
        return self


class DOAJAPIReader(object):
    def __init__(self):
        self.url = "https://doaj.org/api/v1/search/articles/tobacco?page=0&pageSize=100"
        self._start = 0
        self._delay = 10

    @sleep_and_retry
    @limits(calls=5, period=2)
    def __next__(self):
        logger.debug('Fetching DOAJ articles from: {}'.format(self.url))
        if not self.url:
            logger.info('No more articles for `tobacco` from DOAJ. Stopping iteration')
            raise StopIteration()

        response = requests.get(url=self.url)
        if response.status_code == 200:
            content = response.json()
            self.url = content.get('next', None)
            articles = content['results']
            logger.info('DOAJ docs length: {}'.format(len(articles)))
            return articles
        elif response.status_code == 429:
            time.sleep(self._delay)
        else:
            logger.info('DOAJ returned no content. Status Code: {}'.format(response.status_code))
            return []

    def __iter__(self):
        return self


class DOAJValidator(threading.Thread):
    def __init__(self, store, source: queue.Queue,
                 sink: queue.Queue):
        super(DOAJValidator, self).__init__()
        self._store = store
        self._source = source
        self._sink = sink

    def run(self):
        while True:
            task = self._source.get()
            if not task:
                break
            self.validate_and_link(task)
            self._source.task_done()

    def validate_and_link(self, task: Tuple[int, Dict[str, Any]]):
        article_id, plos_article = task
        doi = plos_article.get('id', None)
        if doi:
            doaj_article = self._store.get(doi, None)
            if not doaj_article:
                logging.info('No DOAJ article found for article: {} / doi: {}'.format(article_id, doi))
            else:
                bibjson = doaj_article.get('bibjson', None)
                if not bibjson:
                    logging.info('No bibjson found for article: {} / doi: {}'.format(article_id, doi))
                else:
                    doaj_authors = bibjson.get('author', [])
                    plos_article['doaj_authors'] = doaj_authors
                    plos_article['doaj_id'] = doaj_article['id']
                    self._sink.put((article_id, plos_article))
        else:
            logger.info('No DOI found for article {}, skipping article.'.format(article_id))


class Counters(threading.Thread):
    def __init__(self, source: queue.Queue, authors: Counter,
                 journals: Counter, departments: Counter):
        super(Counters, self).__init__()
        self._source = source
        self._authors = authors
        self._journals = journals
        self._departments = departments

    def run(self):
        while True:
            task = self._source.get()
            if not task:
                break

            self.update_counters(task)
            self._source.task_done()

    def update_counters(self, task: Tuple[int, Dict[str, Any]]):
        article_id, article = task
        doaj_authors = article.get('doaj_authors', [])

        authors = []
        deps = []
        if doaj_authors:
            for author in doaj_authors:
                name = author.get('name', None)
                dep = author.get('affiliation', None)
                if name:
                    authors.append(name)
                if dep:
                    deps.append(dep)
        else:
            plos_authors = article.get('author_display', [])
            for author in plos_authors:
                authors.append(author)

        journals = [article.get('journal', None)]

        logger.info('Updating counts for article: {}'.format(article_id))
        self._authors.update(authors)
        self._departments.update(deps)
        self._journals.update(journals)


def save_csv(counter: Counter, filename: str):
    logger.info('Saving counter: {}'.format(filename))
    with open(filename, 'w', encoding='utf-8') as fh:
        if len(counter):
            for k, v in counter.most_common():
                if not k:
                    continue
                line = ','.join([k, str(v)]) + "\n"
                fh.write(line)


def main():
    # Instantiate and load DOAJ data in memory
    doaj_store = dict()
    doaj_iterator = DOAJAPIReader()
    logger.debug('Starting store setup')

    for articles in doaj_iterator:
        for article in articles:
            bibjson = article.get('bibjson', None)
            if bibjson:
                identifiers = bibjson.get('identifier', [])
                for identifier in identifiers:
                    if identifier['type'].lower() == 'doi':
                        doi = identifier['id']
                        doaj_store[doi] = article

    logger.info('DOAJ store setup complete')

    plos_iterator = PLOSAPIReader()

    source = queue.Queue()
    sink = queue.Queue()  # deque()

    nb_workers = 2

    workers = [DOAJValidator(doaj_store, source, sink) for _ in range(nb_workers)]
    for worker in workers:
        worker.start()

    nb_counters = 2

    authors = Counter()
    departments = Counter()
    journals = Counter()

    counters = [Counters(sink, authors, journals=journals,
                         departments=departments) for _ in range(nb_counters)]
    for counter in counters:
        counter.start()

    page = 0
    for articles in plos_iterator:
        if articles:
            for idx, article in enumerate(articles):
                source.put((page + idx, article))
            page += 100

    source.join()

    # stop workers
    for i in range(nb_workers):
        source.put(None)

    for worker in workers:
        worker.join()

    # stop counters
    for i in range(nb_counters):
        sink.put(None)

    for counter in counters:
        counter.join()

    # save counts
    print(authors)
    print(journals)
    print(departments)

    save_csv(authors, 'authors.csv')
    save_csv(journals, 'journals.csv')
    save_csv(departments, 'departments.csv')


if __name__ == '__main__':
    main()
