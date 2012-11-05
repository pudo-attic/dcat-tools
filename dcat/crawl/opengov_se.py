from lxml import etree
from hashlib import sha1
from dcat.core import *
from base import CatalogCrawler, DatasetCrawler

INDEX_URL = "http://www.opengov.se/feeds/data/"
ATOM_NS = "http://www.w3.org/2005/Atom"

class OpengovSeCatalogCrawler(CatalogCrawler):
    
    def run(self):
        doc = etree.parse(INDEX_URL)
        for link in doc.findall("//{%s}link[@type='application/rdf+xml']" % ATOM_NS):
            self.queue(OpengovSeDatasetCrawler, entry=link.get('href'))

class OpengovSeDatasetCrawler(DatasetCrawler):

    def run(self, entry):
        self.graph.parse(entry, format="xml")
        self.write(sha1(entry).hexdigest())
