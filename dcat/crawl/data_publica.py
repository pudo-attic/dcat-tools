import urllib2, urllib, urlparse
from cookielib import CookieJar
from datetime import datetime
import string
from lxml import html
from StringIO import StringIO

INITIAL_INDEX = "http://www.data-publica.com/en/data/WebSection_viewContentDetailledList"
INDEX_URL = "http://www.data-publica.com/en/data"

from dcat.core import *
from base import CatalogCrawler, DatasetCrawler

class DataPublicaCatalogCrawler(CatalogCrawler):
    
    def run(self, jar=None, url=None, last_urls=None):
        if jar is None: 
            jar = CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(jar))
        url = url or INITIAL_INDEX
        fh = opener.open(url)
        doc = html.parse(fh)
        fh.close()
        
        known_urls = []
        for a in doc.findall(".//div[@class='main']//a"): 
            href = a.get('href').split('?', 1)[0]
            if not href in known_urls:
                self.queue(DataPublicaDatasetCrawler, url=href)
                known_urls.append(href)
        
        if known_urls == last_urls:
            return

        inputs = [] 
        for input in doc.findall(".//form[@id='main_form']//input"):
            inputs.append((input.get('name'), input.get('value')))
        inputs.append(('listbox_nextPage:method', ''))
        next_url = INDEX_URL + '?' + urllib.urlencode(inputs)
        self.queue(DataPublicaCatalogCrawler, jar=jar, 
                   url=next_url, last_urls=known_urls, low_priority=True)


class DataPublicaDatasetCrawler(DatasetCrawler):
    
    def handle_dist(self, dataset, url):
        fh = urllib2.urlopen(url)
        doc = html.document_fromstring(fh.read().decode('utf-8'))
        fh.close()
        dist = URIRef(url)
        id = url.split("/")[-1].decode('utf-8')
        self.graph.add((dist, RDF.type, DCAT.Distribution))
        self.graph.add((dataset, DCAT.distribution, dist))
        self.graph.add((dist, DC.identifier, Literal(id)))
        fmt = BNode()
        self.graph.add((fmt, RDF.type, DC.IMT))
        self.graph.add((dist, DC['format'], fmt))
        for field in doc.findall(".//div"):
            if not 'field' in field.get('class', ''): continue
            name = field.find("label").text.strip()
            if name == 'Title': 
                text = field.find("div").xpath("string()").strip()
                title = Literal(text, lang=u"fr")
                self.graph.add((dist, RDFS.label, title))
            
            if name == 'Type': 
                text = field.find("div").xpath("string()").strip()
                type_ = Literal(text, lang=u"en")
                self.graph.add((fmt, RDFS.label, type_))

            if name == 'Format': 
                text = field.find("div").xpath("string()").strip()
                format_ = Literal(text, lang=u"fr")
                self.graph.add((fmt, RDF.value, format_))

            if name == 'URL': 
                a = field.find("div//a")
                self.graph.add((dist, DCAT.accessURL, URIRef(a.get('href'))))

            if name == 'Description':
                text = field.find("div[@class='input']/div").xpath("string()")
                description = Literal(text.strip(), lang=u"fr")
                self.graph.add((dist, DC.description, description))
            
            #TODO: Metadata, Version


    def pub_list(self, dataset, opener, doc, last_page=[]):
        this_page = []
        for link in doc.findall(".//div[@class='data']/div[@class='main']//a"):
            this_page.append(link.get('href'))

        if last_page == this_page:
            return

        for url in this_page: 
            self.handle_dist(dataset, url)

        inputs = [] 
        for input in doc.findall(".//form[@id='main_form']//input"):
            inputs.append((input.get('name'), input.get('value')))
        inputs.append(('related_link_listbox_nextPage:method', 'Next'))
        url = self.url + '?' + urllib.urlencode(inputs)
        fh = opener.open(url)
        doc_ = html.document_fromstring(fh.read().decode('utf-8'))
        fh.close()
        self.pub_list(dataset, opener, doc_, last_page=this_page)


    def run(self, url):
        self.url = url
        jar = CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(jar))
        fh = opener.open(url)
        doc = html.document_fromstring(fh.read().decode('utf-8'))
        fh.close()
        dataset = URIRef(url)
        id = url.split("/")[-1].decode('utf-8')
        self.graph.add((dataset, RDF.type, DCAT.Dataset))
        self.graph.add((dataset, DC.identifier, Literal(id)))
        for field in doc.findall(".//div"):
            if not 'field' in field.get('class', ''): continue
            name = field.find("label").text.strip()
            #FIELD Title
            if name == 'Title': 
                text = field.find("div").xpath("string()").strip()
                title = Literal(text, lang=u"fr")
                self.graph.add((dataset, DC.title, title))
                self.graph.add((dataset, RDFS.label, title))
            #FIELD Categories
            if name == 'Categories':
                for elem in field.findall("div[@class='input']"):
                    if not elem.text: continue
                    subj = Literal(elem.text.strip(), lang=u"en")
                    self.graph.add((dataset, DC.subject, subj))
            #FIELD Software Licence
            if name == 'Software Licence':
                a = field.find("div/a")
                if a is not None:
                    license = URIRef(a.get('href'))
                    self.graph.add((dataset, DC.rights, license))
                    self.graph.add((license, RDFS.label, Literal(a.text)))
            #FIELD Editor
            if name == 'Editor':
                a = field.find("div/a")

                # HACK - rdf index doesn't support URIs for creator
                #editor = URIRef(a.get('href'))
                #self.graph.add((dataset, DC.creator, editor))
                #self.graph.add((editor, FOAF.name, Literal(a.text)))
                #self.graph.add((editor, RDFS.label, Literal(a.text)))
                self.graph.add((dataset, DC.creator, Literal(a.text)))

            #FIELD Deposit Date
            if name == 'Deposit Date':
                text = field.find("div[@class='input']").xpath("string()")
                text = "".join([c for c in text if c in string.digits+"/:"])
                if len(text.strip()):
                    date = datetime.strptime(text, "%d/%m/%Y%H:%M")
                    date = Literal(date.isoformat())
                    self.graph.add((dataset, DC.issued, date))
            #FIELD Update Date
            if name == 'Update Date':
                text = field.find("div[@class='input']").xpath("string()")
                text = "".join([c for c in text if c in string.digits+"/:"])
                if len(text.strip()):
                    date = datetime.strptime(text, "%d/%m/%Y%H:%M")
                    date = Literal(date.isoformat())
                    self.graph.add((dataset, DC.modified, date))
            #FIELD Frequency Update
            if name == 'Frequency Update':
                text = field.find("div[@class='input']").xpath("string()")
                freq = Literal(text.strip(), lang=u"en")
                self.graph.add((dataset, DC.accrualPeriodicity, freq))
            #FIELD Tags
            if name == 'Tags':
                for elem in field.find("div[@class='input']/div").iter():
                    tag = None
                    if elem.text:
                        tag = elem.text.strip()
                    if elem.tail:
                        tag = elem.tail.strip()
                    if tag:
                        self.graph.add((dataset, DCAT.keyword, Literal(tag, lang=u"fr")))
            #FIELD State
            #FIELD Description
            if name == 'Description':
                text = field.find("div[@class='input']/div").xpath("string()")
                description = Literal(text.strip(), lang=u"fr")
                self.graph.add((dataset, DC.description, description))
            #FIELD URL
            if name == 'URL':
                link = URIRef(field.find("div/a").get('href'))
                self.graph.add((dataset, FOAF.homepage, link))

            #FIELD Data Publications
            if name == 'Data Publications':
                self.pub_list(dataset, opener, doc)
            #print "FIELD", name.encode('utf-8')  ##, "VAL", value.encode('utf-8')
        self.write(id)

