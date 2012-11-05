import re
from ckanclient import CkanClient
from ckanrdf.package import Package
from base import CatalogCrawler, DatasetCrawler

from dcat.core import *

class CkanClientMixIn(object):
    
    @property
    def ckan(self):
        if not hasattr(self, '__ckan'):
            base = self.config.get('ckan_base', 'http://ckan.net/')
            if not base.endswith('/'): 
                base += "/"
            self.__ckan = CkanClient(base_location=base  + "api",
                                     api_key=self.config.get('api_key'),
                                     http_user=self.config.get('http_user'),
                                     http_pass=self.config.get('http_pass'))    
        return self.__ckan


class CkanCatalogCrawler(CatalogCrawler, CkanClientMixIn):
    
    def run(self):
        for package in self.ckan.package_register_get():
            self.queue(CkanDatasetCrawler, package=package)
        
        
class CkanDatasetCrawler(DatasetCrawler, CkanClientMixIn):
    
    def run(self, package):
        pkg = self.ckan.package_entity_get(package)
        rdf_writer = Package(self.config)
        self.graph += rdf_writer.describe(pkg, store=self.graph.store)
           
        # HACK 1: fix DGU spatial names
        if "data.gov.uk" in self.config.get('ckan_base'):
            spatials = list(self.graph.triples((None, DC.spatial, None)))
            self.graph.remove((None, DC.spatial, None))
            for (s, p, o) in spatials:
                place = o.split(":")[-1]
                places = re.split(r"[,\(\)]", place)
                places = filter(len, map(lambda p: p.strip(), places))
                for place in places:
                    self.graph.add((s, p, Literal(place)))
        
        #for dataset, _, _ in self.graph.triples((None, RDF.type, DCAT.Dataset)):

        self.write(package)
