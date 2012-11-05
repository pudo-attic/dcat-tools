import urllib2, urllib, urlparse
from cookielib import CookieJar
from datetime import datetime
import string
from lxml import html
from StringIO import StringIO

INDEX_URL = "http://dati.piemonte.it/dati.html?limit=1000000"

from dcat.core import *
from base import CatalogCrawler, DatasetCrawler

class PiemonteCatalogCrawler(CatalogCrawler):
    
    def run(self):
        doc = html.parse(INDEX_URL)
        for item in doc.findall("//div[@class='datiItem'/a]"):
            self.queue(PiemonteDatasetCrawler, url=a.get('href'))

class PiemonteDatasetCrawler(DatasetCrawler):
    
    def run(self, url):
        dataset = URIRef(url)
        id = url.split("/")[-1].decode('utf-8')
        self.graph.add((dataset, RDF.type, DCAT.Dataset))
        self.graph.add((dataset, DC.identifier, Literal(id)))
