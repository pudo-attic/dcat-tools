from urlparse import urljoin
from lxml import etree, html

from base import CatalogCrawler, DatasetCrawler


class XMLParserMixIn(object):
    
    def load_doc(self, url):
        return etree.parse(url)
    
    def handle_doc(self, doc, url=None, **kwargs):
        raise ValueError("No handler: %s" % repr(doc))
        
    def run(self, url=None, **kwargs):
        if url is not None:
            return self.handle_doc(self.load_doc(url), url=url)
        raise ValueError("No URL is specified!")

class HTMLParserMixIn(XMLParserMixIn):
    
    def load_doc(self, url):
        return html.parse(url)
    
class XPathIndex(object):
    
    def run(self, url=None, **kwargs):
        if url is None:
            url = self.config.get('index_url')
        if url is None:
            raise ValueError("No URL for index specified!")
        return self.handle_doc(self.load_doc(url), url=url)
        
    def handle_doc(self, doc, url=None):
        record_xpath = self.config.get('record_xpath', '//a').strip()
        record_type = self.config.get('record_type')
        for link in doc.xpath(record_xpath):
            assert link.tag=='a', repr(link)
            _url = link.get('href')
            if url: 
                _url = urljoin(url, _url)
            self.queue(record_type, url=_url)


class XMLCatalogCrawler(XPathIndex, XMLParserMixIn, CatalogCrawler):
    pass
    
class HTMLCatalogCrawler(XPathIndex, HTMLParserMixIn, CatalogCrawler):
    pass
            
class XMLDatasetCrawler(XMLParserMixIn, DatasetCrawler):
    pass

class HTMLDatasetCrawler(HTMLParserMixIn, DatasetCrawler):
    pass