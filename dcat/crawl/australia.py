from lxml import html
from hashlib import sha1
from dcat.core import *
from base import CatalogCrawler, DatasetCrawler

INDEX_URL = "http://data.gov.au/data/"

class DataGovAuCatalogCrawler(CatalogCrawler):
    
    def run(self):
        doc = html.parse(INDEX_URL)
        for link in doc.findall("//a[@class='result-title']"):
            self.queue(DataGovAuDatasetCrawler, entry=link.get('href'))

class DataGovAuDatasetCrawler(DatasetCrawler):

    def run(self, entry):
        self.graph.parse(entry, format="rdfa")
        self.write(sha1(entry).hexdigest())





