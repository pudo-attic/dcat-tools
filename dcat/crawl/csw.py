__import__("pkg_resources").get_distribution("lxml>2.2.4")

from pprint import pprint

from owslib.csw import CatalogueServiceWeb
from owslib.csw import namespaces as csw_ns
from dcat.core import *
from base import CatalogCrawler, DatasetCrawler

PAGE_SIZE = 100

CSWNS = Namespace("urn:csw#")

class CSWCatalogCrawler(CatalogCrawler):
    
    def run(self, csw=None, offset=0):
        if csw is None:
            endpoint = self.config['endpoint']
            csw = CatalogueServiceWeb(endpoint)
        outputschema = self.config.get('outputschema', csw_ns['gmd'])
        typenames = self.config.get('typenames', 'csw:dataset')
        csw.getrecords(esn="full", outputschema=outputschema,
                typenames=typenames, startposition=offset, 
                maxrecords=PAGE_SIZE)
        for record in csw.records.values(): 
            self.queue(CSWDatasetCrawler, record=record)
        
        if csw.results.get('nextrecord') <= csw.results.get('matches'):
            print "OFFSET", offset
            self.queue(CSWCatalogCrawler, csw=csw, 
                       offset=csw.results.get('nextrecord'))

class CSWDatasetCrawler(DatasetCrawler):
    
    def handle_contact(self, src, lang):
        contact = BNode() 
        self.graph.add((contact, RDF.type, VCARD.VCard))
        if src.organization:
            self.graph.add((contact, VCARD['organisation-name'], 
                Literal(src.organization)))
            self.graph.add((contact, RDFS.label, 
                Literal(src.organization)))
        if src.name:
            self.graph.add((contact, VCARD.fn, 
                Literal(src.name)))
        if src.position:
            self.graph.add((contact, VCARD.title, 
                Literal(src.position, lang=lang)))
        if src.email:
            self.graph.add((contact, VCARD.email, 
                Literal(src.email)))
        if src.city:
            self.graph.add((contact, VCARD.locality, 
                Literal(src.city)))
        if src.postcode:
            self.graph.add((contact, VCARD['postal-code'], 
                Literal(src.postcode)))
        if src.address:
            self.graph.add((contact, VCARD['street-address'], 
                Literal(src.address)))
        if src.country:
            self.graph.add((contact, VCARD['country-name'], 
                Literal(src.country)))
        if src.phone:
            self.graph.add((contact, VCARD.tel, 
                Literal(src.phone)))
        if src.fax:
            self.graph.add((contact, VCARD.fax, 
                Literal(src.fax)))
        if src.onlineresource:
            self.graph.add((contact, FOAF.homepage, 
                URIRef(src.onlineresource.url)))
        return contact
    
    def handle_bbox(self, src, lang):
        bbox = BNode()
        if src.maxx and src.maxy and src.minx and src.miny:
            self.graph.add((bbox, RDF.type, CSWNS.BoundingBox))
            self.graph.add((bbox, CSWNS.maxX, Literal(src.maxx)))
            self.graph.add((bbox, CSWNS.maxY, Literal(src.maxy)))
            self.graph.add((bbox, CSWNS.minX, Literal(src.minx)))
            self.graph.add((bbox, CSWNS.minY, Literal(src.miny)))
        return bbox
    
    def run(self, record):
        self.graph.bind("csw", CSWNS)
        lang = record.language or 'en'
        dataset = URIRef("urn:csw:" + record.identifier)
        self.graph.add((dataset, RDF.type, DCAT.Dataset))
        self.graph.add((dataset, DC.identifier, 
            Literal(record.identifier)))
        ifn = record.identification
        if ifn.title is not None:
            self.graph.add((dataset, DC.title, 
                Literal(ifn.title, lang=lang)))
        if ifn.abstract is not None:
            self.graph.add((dataset, DC.description, 
                Literal(ifn.abstract, lang=lang)))
        if ifn.topiccategory is not None:
            self.graph.add((dataset, DC.subject, 
                Literal(ifn.topiccategory, lang=lang)))
        if ifn.date is not None:
            self.graph.add((dataset, DC.created, 
                Literal(ifn.date)))
        if ifn.keywords is not None:
            for keyword in ifn.keywords.get('list', []):
                self.graph.add((dataset, DC.keyword, 
                    Literal(keyword, lang=lang)))
        
        if record.contact:
            contact = self.handle_contact(record.contact, lang)
            self.graph.add((dataset, DC.creator, contact))
        elif ifn.contact:
            contact = self.handle_contact(ifn.contact, lang)
            self.graph.add((dataset, DC.creator, contact))
        
        if ifn.bbox:
            bbox = self.handle_bbox(ifn.bbox, lang)
            self.graph.add((dataset, DC.extent, bbox))
        elif hasattr(ifn, 'service') and ifn.service.bbox:
            bbox = self.handle_bbox(ifn.service.bbox, lang)
            self.graph.add((dataset, DC.extent, bbox))
        
        if hasattr(record, 'distribution') and record.distribution:
            for online in record.distribution.online:
                dist = URIRef(online.url)
                self.graph.add((dist, RDF.type, DCAT.Distribution))
                self.graph.add((dataset, DCAT.distribution, dist))
                self.graph.add((dist, DCAT.accessURL, dist))
                if record.distribution.version:
                    self.graph.add((dist, DC.hasVersion, 
                        Literal(record.distribution.version, lang=lang)))
                if record.distribution.__dict__.get('format'):
                    self.graph.add((dist, DC['format'],
                        Literal(record.distribution.__dict__.get('format'))))
                if online.name:
                    self.graph.add((dist, DC.title, 
                        Literal(online.name, lang=lang)))
                    self.graph.add((dist, RDFS.label, 
                        Literal(online.name, lang=lang)))
                if online.description:
                    self.graph.add((dist, DC.description, 
                        Literal(online.description, lang=lang)))

        if hasattr(ifn, 'service') and ifn.service:
            for operation in ifn.service.operations:
                for online in operation.get('connectpoint', []):
                    dist = URIRef(online.url)
                    self.graph.add((dist, RDF.type, DCAT.Distribution))
                    self.graph.add((dataset, DCAT.distribution, dist))
                    self.graph.add((dist, DCAT.accessURL, dist))
                    self.graph.add((dist, DC['format'], Literal("WMS")))
                    if operation.get('name'):
                        self.graph.add((dist, DC.title, 
                            Literal(operation.get('name'), lang=lang)))
                        self.graph.add((dist, RDFS.label, 
                            Literal(operation.get('name'), lang=lang)))

        #print "RECORD"
        #pprint(record.__dict__)
        #print "IDENT"
        #pprint(record.identification.__dict__)
        self.write(record.identifier)
