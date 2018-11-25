import requests
import threading
import queue
import time

from ratelimit import limits, sleep_and_retry
from collections import Counter, deque

from typing import Dict, Any, Tuple
import csv

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
        logger.debug('Fetching articles starting from {}'.format(self._start))
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


sample_json = {'id': '10.1371/journal.pone.0076005',
               'journal': 'PLoS ONE',
               'eissn': '1932-6203',
               'publication_date': '2013-10-01T00:00:00Z',
               'article_type': 'Research Article',
               'author_display': ['Kolappan Chockalingam',
                                  'Chandrasekaran Vedhachalam',
                                  'Subramani Rangasamy',
                                  'Gomathi Sekar',
                                  'Srividya Adinarayanan',
                                  'Soumya Swaminathan',
                                  'Pradeep Aravindan Menon'],
               'abstract': [
                   'Background: Tobacco use leads to many health complications and is a risk factor for the occurrence of cardio vascular diseases, lung and oral cancers, chronic bronchitis etc. Almost 6 million people die from tobacco-related causes every year. This study was conducted to measure the prevalence of tobacco use in three different areas around Chennai city, south India. Methods: A survey of 7510 individuals aged >\u200a=\u200a15 years was undertaken covering Chennai city (urban), Ambattur (semi-urban) and Sriperumbudur (rural) taluk. Details on tobacco use were collected using a questionnaire adapted from both Global Youth Tobacco Survey and Global Adults Tobacco Survey. Results: The overall prevalence of tobacco use was significantly higher in the rural (23.7%) compared to semi-urban (20.9%) and urban (19.4%) areas (P value <0.001) Tobacco smoking prevalence was 14.3%, 13.9% and 12.4% in rural, semi-urban and urban areas respectively. The corresponding values for smokeless tobacco use were 9.5%, 7.0% and 7.0% respectively. Logistic regression analysis showed that the odds of using tobacco (with smoke or smokeless forms) was significantly higher among males, older individuals, alcoholics, in rural areas and slum localities. Behavioural pattern analysis of current tobacco users led to three groups (1) those who were not reached by family or friends to advice on harmful effects (2) those who were well aware of harmful effects of tobacco and even want to quit and (3) those are exposed to second hand/passive smoking at home and outside. Conclusions: Tobacco use prevalence was significantly higher in rural areas, slum dwellers, males and older age groups in this region of south India. Women used mainly smokeless tobacco. Tobacco control programmes need to develop strategies to address the different subgroups among tobacco users. Public health facilities need to expand smoking cessation counseling services as well as provide pharmacotherapy where necessary. '],
               'title_display': 'Prevalence of Tobacco Use in Urban, Semi Urban and Rural Areas in and around Chennai City, India',
               'score': 6.777194}


class TestReader(object):
    def __init__(self, n):
        self.n = n
        self._start = 0

    @sleep_and_retry
    @limits(calls=300, period=60 * 60)
    @limits(calls=10, period=60)
    def __next__(self):
        if self._start < self.n:
            self._start += 100
            return [sample_json] * 200
        else:
            raise StopIteration()

    def __iter__(self):
        return self


class DOAJValidator(threading.Thread):
    def __init__(self, source: queue.Queue, sink):
        super(DOAJValidator, self).__init__()
        self._source = source
        self._sink = sink
        self._delay = 10

        self._doaj_url = "https://doaj.org/api/v1/search/articles/doi:{}"

    def run(self):
        while True:
            task = self._source.get()
            if not task:
                break
            self.validate_and_link(task)
            self._source.task_done()

    def validate_and_link(self, task: Tuple[int, Dict[str, Any]]):
        article_id, article = task
        doi = article.get('id', None)
        if doi:
            logger.debug('Querying DOAJ for doi: {}'.format(doi))
            response = requests.get(url=self._doaj_url.format(doi))
            if response.status_code == 200:
                results = response.json().get('results', None)
                if results:
                    doaj_article = results[0]
                    article['doaj_id'] = doaj_article['id']
                    article['doaj_authors'] = doaj_article['bibjson'].get('author', [])
                    self._sink.put((article_id, article))
                    logger.info('queued article: {} / doi: {} for further processing'.format(article_id, doi))
                else:
                    logger.info('DOAJ Query did not return results for doi: {}, skipping article'.format(doi))
            elif response.status_code == 429:
                # Throttle processing
                logger.info('DOAJ Server returned status {}, waiting for {}s before next request'.format(
                    response.status_code,
                    self._delay))
                self._source.put(task)
                time.sleep(self._delay)
            else:
                logger.info('Query returned status {} for doi: {}'.format(response.status_code, doi))
                self._source.put(task)
        else:
            logger.info('No DOI found for article {}, skipping article'.format(article_id))


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
            article = self._source.get()
            if not article:
                break

            self.update_counters(article)
            self._source.task_done()

    def update_counters(self, task):
        article_id, article = task
        doaj_authors = article.get('doaj_authors', [])
        doaj_id = article.get('doaj_id', None)
        plos_id = article.get('id', None)

        authors = []
        deps = []
        if doaj_authors:
            for author in doaj_authors:
                name = author.get('name', None)
                dep = author.get('affiliation', None)
                if name:
                    authors.append((name, doaj_id, plos_id))
                if dep:
                    deps.append((dep, doaj_id, plos_id))
        else:
            plos_authors = article.get('author_display', [])
            for author in plos_authors:
                authors.append((author, doaj_id, plos_id))

        journals = [(article.get('journal', None), doaj_id, plos_id)]

        logger.info('Updating counts for article: {}'.format(article_id))
        self._authors.update(authors)
        self._departments.update(departments)
        self._journals.update(journals)


def save_csv(counter: Counter, filename: str):
    logger.info('Saving counter: {}'.format(filename))
    with open(filename, 'w') as fh:
        csv_fh = csv.writer(fh)
        for k, v in counter.most_common():
            csv_fh.writerow(list(k) + [v])


if __name__ == '__main__':
    # reader = TestReader(1)
    reader = PLOSAPIReader()

    source = queue.Queue()
    sink = queue.Queue()  # deque()

    nb_workers = 2

    workers = [DOAJValidator(source, sink) for _ in range(nb_workers)]
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
    for articles in reader:
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
    save_csv(authors, 'authors.csv')
    save_csv(departments, 'departments.csv')
    save_csv(journals, 'journals.csv')
