from datetime import datetime
import re
import urllib2
import string
from SOAPpy import WSDL
from threading import Lock
from datetime import datetime
from lxml import html

RECHERCHE_URL = "https://www-genesis.destatis.de/genesisWS/services/RechercheService_2009?wsdl"
BASE_URL = "https://www-genesis.destatis.de/genesis/online"

from dcat.core import *
from base import CatalogCrawler, DatasetCrawler

lock = Lock()

class GenesisCatalogCrawler(CatalogCrawler):
    
    def run(self, offset=0, recherche=None):
        if recherche is None: 
            recherche = WSDL.Proxy(self.config.get("recherche_url", RECHERCHE_URL))
        try:
            lock.acquire()
            katalog = recherche.StatistikKatalog(
                    self.config.get("user"), 
                    self.config.get("password"), 
                    "%s*" % offset, "Code", "500", "de")
        finally:
            lock.release()
        try:
            for entry in katalog.statistikKatalogEintraege:
                self.queue(GenesisDatasetCrawler, statistic=entry,
                        recherche=recherche)
        except: pass
        if offset == 9: 
            return 
        self.queue(GenesisCatalogCrawler, offset=offset+1, 
                recherche=recherche, low_priority=True)

class GenesisDatasetCrawler(DatasetCrawler):
    
    def run(self, statistic, recherche):
        url = statistic.links[0].href
        dataset = URIRef(url)
        self.graph.add((dataset, RDF.type, DCAT.Dataset))
        self.graph.add((dataset, DC.identifier, Literal(statistic.code)))
        self.graph.add((dataset, DC.title, Literal(statistic.inhalt, lang="de")))
        
        fh = urllib2.urlopen(url)
        doc = html.parse(fh)
        fh.close()
        desc = [n.text for n in doc.findall("//pre[@class='langtext']")]
        if desc:
            self.graph.add((dataset, DC.description, 
                Literal("\n".join(desc), lang="de")))
        
        date = doc.find("//meta[@name='DC.date']").get("content")
        self.graph.add((dataset, DC.date, Literal(date)))
        publisher = doc.find("//meta[@name='DC.publisher']").get("content")
        self.graph.add((dataset, DC.publisher, Literal(publisher, lang="de")))
        self.graph.add((dataset, DC.creator, Literal(publisher, lang="de")))

        try:
            lock.acquire()
            tables = recherche.TabellenKatalog(
                    self.config.get("user"), 
                    self.config.get("password"), 
                    "%s*" % statistic.code, 
                    "Alle", "500", "de")
            for table in tables.tabellenKatalogEintraege:
                try:
                    for link in table.links:
                        if link.zieltyp == 'Webservice':
                            continue
                        dist = URIRef(link.href)
                        self.graph.add((dist, RDF.type, DCAT.Distribution))
                        self.graph.add((dataset, DCAT.distribution, dist))
                        self.graph.add((dist, DC['format'],
                            Literal(link.contenttype.split(";")[0])))
                        self.graph.add((dist, DC.identifier, Literal(table.code)))
                        self.graph.add((dist, DC.title, 
                            Literal(table.inhalt, lang="de")))
                        self.graph.add((dist, RDFS.label, 
                            Literal(table.inhalt, lang="de")))
                        self.graph.add((dist, DC.description, 
                            Literal(link.titel, lang="de")))
                except: pass
        finally:
            lock.release()

        self.write(statistic.code)
