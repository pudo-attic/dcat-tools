from urllib2 import urlopen
from urlparse import urljoin
from datetime import datetime
try:
    import json 
except ImportError:
    import simplejson as json

from dcat.core import *
from base import CatalogCrawler, DatasetCrawler

ORGS = {} 

def dict_to_date(d):
    try:
        return datetime(int(d.get('year')), int(d.get('month', 1)), int(d.get('day', 1)))
    except:
        return None

class NationalDataCatalogGetter(object):
    
    def get(self, suffix):
        api_key = self.config.get('api_key')
        endpoint = self.config.get('endpoint', 'http://api.nationaldatacatalog.com/')
        url = urljoin(endpoint, suffix) + "?api_key=%s" % api_key
        urlfh = urlopen(url)
        try:
            out = json.load(urlfh)
            if out is None:
                raise ValueError(url)
            return out
        finally:
            urlfh.close()

class NationalDataCatalogCrawler(CatalogCrawler, NationalDataCatalogGetter):
    
    def run(self, url=None):
        if url is None:
            url = "/sources"
        data = self.get(url)
        
        for dataset in data.get('members', []):
            self.queue(NationalDataCatalogDatasetCrawler, data=dataset)
        
        if data.get('next'):
            next = data.get('next', {}).get('href')
            if next is not None:
                next = urljoin(url, next)
                self.queue(NationalDataCatalogCrawler, url=next, low_priority=True)

        
class NationalDataCatalogDatasetCrawler(DatasetCrawler, NationalDataCatalogGetter):
    
    def get_org(self, spec):
        if not spec.get('href') in ORGS.keys():
            ORGS[spec.get('href')] = self.get(spec.get('href'))
        data = ORGS.get(spec.get('href'))
        url = data.get('home_url', 
                data.get('url', 
                    data.get('catalog_url')))
        org = URIRef(url) if url else BNode()
        self.graph.add((org, RDF.type, FOAF.Organization))
        self.graph.add((org, DC.title, Literal(data.get('name'))))
        self.graph.add((org, RDFS.label, Literal(data.get('name'))))
        self.graph.add((org, DC.identifier, Literal(data.get('slug'))))
        if data.get('description'):
            self.graph.add((org, DC.description, Literal(data.get('description'))))
        self.graph.add((org, DC.type, Literal(data.get('org_type'))))
        if data.get('home_url'):
            self.graph.add((org, FOAF.homepage, URIRef(data.get('home_url'))))
        if data.get('url'):
            self.graph.add((org, FOAF.page, URIRef(data.get('url'))))
        return org
    
    def run(self, data):
        name = data.get('catalog_name') + "-" + data.get('slug')
        dataset = URIRef(data.get('url'))
        self.graph.add((dataset, RDF.type, DCAT.Dataset))
        
        self.graph.add((dataset, DC.identifier, Literal(data.get('slug'))))
        self.graph.add((dataset, OWL.sameAs, UUID[data.get('id')]))
        self.graph.add((dataset, DC.title, Literal(data.get('title'))))
        if data.get('description'):
            self.graph.add((dataset, DC.description, Literal(data.get('description'))))
        
        publisher = URIRef(data.get('catalog_url'))
        self.graph.add((dataset, DC.publisher, publisher))
        self.graph.add((publisher, RDFS.label, Literal(data.get('catalog_name'))))
        
        if data.get('created_at'):
            self.graph.add((dataset, DC.created, Literal(data.get('created_at'))))
        if data.get('updated_at'):
            self.graph.add((dataset, DC.date, Literal(data.get('updated_at'))))
        
        org = data.get('organization')
        if org is not None and 'href' in org:
            org = self.get_org(org)
            self.graph.add((dataset, DC.creator, org))
        
        jur = data.get('jurisdiction')
        if jur is not None and 'href' in jur:
            org = self.get_org(jur)
            self.graph.add((dataset, DC.creator, org))
            self.graph.add((dataset, DC.spatial, Literal(jur.get('name'))))
        
        if data.get('license_url'):
            license = URIRef(data.get('license_url'))
            self.graph.add((dataset, DC.license, license))
            if data.get('license'):
                self.graph.add((license, RDFS.label, Literal(data.get('license'))))
        elif data.get('license'):
             self.graph.add((dataset, DC.license, Literal(data.get('license'))))
        
        if data.get('documentation_url'):
            self.graph.add((dataset, DCAT.dataDictionary, URIRef(data.get('documentation_url'))))
        
        if data.get('updates_per_year'):
            self.graph.add((dataset, DCAT.accrualPeriodicity, 
                Literal(data.get('updates_per_year'))))
        
        if data.get('frequency'):
            self.graph.add((dataset, DCAT.accrualPeriodicity, 
                Literal(data.get('frequency'))))
        
        for download in data.get('downloads', []):
            dist = URIRef(urljoin(dataset, download.get('href')))
            self.graph.add((dist, RDF.type, DCAT.Distribution))
            self.graph.add((dataset, DCAT.distribution, dist))
            self.graph.add((dist, DC['format'], Literal(download.get('format'))))
            if download.get('size'):
                self.graph.add((dist, DCAT.size, Literal(download.get('size'))))
            accessURL = URIRef(download.get('url'))
            self.graph.add((dist, DCAT.accessURL, accessURL))
        
        released = dict_to_date(data.get('released'))
        if released:
            self.graph.add((dataset, DC.date, Literal(released)))
        
        self.write(name)