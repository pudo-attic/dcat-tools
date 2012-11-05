__import__("pkg_resources").get_distribution("lxml>2.2.4")

from urlparse import urlparse, urljoin
from pprint import pprint

from dcat.core import *
from base import CatalogCrawler, DatasetCrawler

from lxml import html
from datetime import datetime
import string

INDEX_URL = "http://www.gesetze-im-internet.de/Teilliste_%s.html"

class JurisCatalogCrawler(CatalogCrawler):

    def run(self):
        for suffix in string.uppercase + string.digits:
            doc = html.parse(INDEX_URL % suffix)
            for link in doc.findall('//div[@id="container"]//a'):
                url = urljoin(INDEX_URL, link.get('href'))
                if url.endswith('.pdf'): 
                    continue
                page = html.parse(url)
                urls = [(a.text_content(), urljoin(url, a.get('href'))) \
                        for a in page.findall("//h2[@class='headline']/a")]
                url = urls[0][1]
                #print url
                self.queue(JurisDatasetCrawler, url=url, resources=urls)

class JurisDatasetCrawler(DatasetCrawler):
    
    def run(self, url, resources):
        doc = html.parse(url)
        #print url
        dataset = URIRef(url)
        self.graph.add((dataset, RDF.type, DCAT.Dataset))
        gin = URIRef("http://www.gesetze-im-internet.de/")
        self.graph.add((dataset, DC.publisher, gin))
        self.graph.add((gin, RDFS.label, Literal("Gesetze im Internet")))
        bmj = URIRef("http://www.bmj.de/")
        self.graph.add((dataset, DC.creator, bmj))
        self.graph.add((bmj, RDFS.label, Literal("Bundesministerium der Justiz")))
        
        for format_, link in resources: 
            dist = URIRef(url + "#" + format_)
            self.graph.add((dist, RDF.type, DCAT.Distribution))
            self.graph.add((dataset, DCAT.distribution, dist))
            self.graph.add((dist, DC['format'], Literal(format_)))
            self.graph.add((dist, DCAT.accessURL, URIRef(link)))

        id_ = None
        for norm in doc.findall("//div[@class='jnnorm']"):
            if norm.get('title') != "Rahmen":
                continue
            full_title = norm.findtext(".//span[@class='jnlangue']")
            short_title = norm.findtext(".//span[@class='jnkurzueamtabk']")
            abbr, date = norm.findall(".//div[@class='jnheader']/p")
            abbr = abbr.text.strip()
            date = date.text.encode('utf-8').strip().split(": ")[-1]
            date = datetime.strptime(date, "%d.%m.%Y")
            self.graph.add((dataset, DC.created, Literal(date.isoformat())))
            if short_title:
                titles = short_title.replace(')', '').replace('(', '').split(" - ")
                title, abbr = [p.strip() for p in titles]
            elif norm.findtext(".//span[@class='jnkurzue']"):
                title = norm.findtext(".//span[@class='jnkurzue']")
            else:
                title = full_title
            id_ = norm.get('id')
            self.graph.add((dataset, DC.identifier, Literal(abbr)))
            self.graph.add((dataset, DC.title, Literal(title.strip())))
            self.graph.add((dataset, RDFS.label, Literal(title.strip())))
            self.graph.add((dataset, DC.description, Literal(full_title.strip())))
            citation = norm.find(".//div[@class='jnzitat']")
            if citation is not None and len(citation.getchildren()) > 1:
                citation = citation.getchildren()[1].text_content()
                self.graph.add((dataset, DC.bibliographicCitation,
                    Literal(citation)))
        self.write(id_)

