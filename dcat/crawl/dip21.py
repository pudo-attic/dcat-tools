__import__("pkg_resources").get_distribution("lxml>2.2.4")

import urllib2, urllib, re
from urlparse import parse_qs, urlparse, urljoin
import logging
from StringIO import StringIO
from cookielib import CookieJar
from datetime import datetime
from threading import Lock
from itertools import count
from lxml import html, etree

from dcat.core import *
from base import CatalogCrawler, DatasetCrawler

MAKE_SESSION_URL = "http://dipbt.bundestag.de/dip21.web/bt"
BASE_URL = "http://dipbt.bundestag.de/dip21.web/searchProcedures/simple_search.do?method=Suchen&offset=%s&anzahl=100"
ABLAUF_URL = "http://dipbt.bundestag.de/dip21.web/searchProcedures/simple_search_list.do?selId=%s&method=select&offset=100&anzahl=100&sort=3&direction=desc"
DETAIL_VP_URL = "http://dipbt.bundestag.de/dip21.web/searchProcedures/simple_search_detail_vp.do?vorgangId=%s"

DIPNS = Namespace("http://dipbt.bundestag.de/ns#")

jar = None
lock = Lock()

def get_dip_with_cookie(url, method='GET', data={}):
    class _Request(urllib2.Request):
        def get_method(self): 
            return method
    lock.acquire()
    try:
        def _req(url, jar, data={}):
            _data = urllib.urlencode(data) 
            req = _Request(url, _data)
            jar.add_cookie_header(req)
            fp = urllib2.urlopen(req)
            jar.extract_cookies(fp, req)
            return fp
        global jar
        if jar is None:
            jar = CookieJar()
            fp = _req(MAKE_SESSION_URL, jar)
            fp.read()
            fp.close()
        return _req(url, jar, data=data)
    finally:
        lock.release()

inline_re = re.compile(r"<!--(.*?)-->", re.M + re.S)
inline_comments_re = re.compile(r"<-.*->", re.M + re.S)

def inline_xml_from_page(page):
    for comment in inline_re.findall(page):
        comment = comment.strip()
        if comment.startswith("<?xml"):
            comment = inline_comments_re.sub('', comment)
            return etree.parse(StringIO(comment))


class DIP21CatalogCrawler(CatalogCrawler):
    
    def run(self, offset=0):
        urlfp = get_dip_with_cookie(BASE_URL % (offset*100))
        doc = html.parse(urlfp)
        table = doc.find(".//table[@summary='Ergebnisliste']")
        if table is None: 
            return
        for result in table.findall(".//a[@class='linkIntern']"):
            url = urljoin(BASE_URL, result.get('href'))
            id = parse_qs(urlparse(url).query).get('selId')[0]
            self.queue(DIP21DatasetCrawler, url=url)
        self.queue(DIP21CatalogCrawler, offset=offset+1)

class DIP21DatasetCrawler(DatasetCrawler):

    def activities(self, dataset, id):
        urlfp = get_dip_with_cookie(DETAIL_VP_URL % id)
        xml = inline_xml_from_page(urlfp.read())
        urlfp.close()
        if xml is None: 
            return
        for position in xml.findall(".//VORGANGSPOSITION"):
            if not position.findtext("FUNDSTELLE_LINK"):
                continue
            dist = URIRef(position.findtext("FUNDSTELLE_LINK"))
            if not (dist, RDF.type, DCAT.Distribution) in self.graph:
                self.graph.add((dist, RDF.type, DCAT.Distribution))
                self.graph.add((dataset, DCAT.distribution, dist))
                self.graph.add((dist, DC['format'], Literal("PDF")))
                house = position.findtext("ZUORDNUNG")
                if house == 'BT':
                    bt = URIRef("http://www.bundestag.de")
                    self.graph.add((dataset, DC.publisher, bt))
                    self.graph.add((bt, RDFS.label, Literal("Deutscher Bundestag")))
                if house == 'BR':
                    br = URIRef("http://www.bundesrat.de")
                    self.graph.add((dataset, DC.publisher, br))
                    self.graph.add((br, RDFS.label, Literal("Bundesrat")))
                source = position.findtext("FUNDSTELLE")
                if source:
                    self.graph.add((dist, DC.type, Literal(source)))
                label = position.findtext("URHEBER")
                if label:
                    self.graph.add((dist, RDFS.label, Literal(label)))

            for creator in position.findall("PERSOENLICHER_URHEBER"):
                cre = BNode()
                self.graph.add((cre, RDF.type, FOAF.Person))
                self.graph.add((dist, DC.creator, cre))
                self.graph.add((cre, FOAF.givenName, 
                    Literal(creator.findtext("VORNAME"))))
                self.graph.add((cre, FOAF.familyName, 
                    Literal(creator.findtext("NACHNAME"))))
                if creator.findtext("FUNKTION"):
                    self.graph.add((cre, FOAF.title, 
                        Literal(creator.findtext("FUNKTION"))))
                if creator.findtext("FRAKTION"):
                    self.graph.add((cre, DIPNS.faction, 
                        Literal(creator.findtext("FRAKTION"))))
                if creator.findtext("RESSORT"):
                    self.graph.add((cre, DIPNS.department, 
                        Literal(creator.findtext("RESSORT"))))
                activity = creator.findtext("AKTIVITAETSART")
                self.graph.add((cre, DC.type, Literal(activity)))

                state = creator.findtext("BUNDESLAND")
                if state:
                    self.graph.add((cre, DC.spatial, Literal(state)))
                page = creator.findtext("SEITE")
                if page:
                    self.graph.add((cre, DC.extent, Literal(page)))

            for delegation in position.findall("ZUWEISUNG"):
                com = delegation.findtext("AUSSCHUSS_KLARTEXT")
                self.graph.add((dist, DIPNS.delegation, Literal(com, lang="de")))
                if delegation.find("FEDERFUEHRUNG") is not None:
                    self.graph.add((dist, DIPNS.delegationLead, Literal(com,
                        lang="de")))
            for decision in position.findall("BESCHLUSS"):
                dec = BNode()
                self.graph.add((dec, RDF.type, DIPNS.Decision))
                self.graph.add((dist, DIPNS.decision, dec))
                reldoc = decision.findtext("BEZUGSDOKUMENT")
                if reldoc:
                    self.graph.add((dec, DC.source, URIRef(reldoc)))
                    page = decision.findtext("BESCHLUSSSEITE")
                    if page:
                        self.graph.add((dec, DC.extent, Literal(page)))
                result = decision.findtext("BESCHLUSSTENOR")
                if result: 
                    self.graph.add((dec, DIPNS.result, Literal(result)))

    def run(self, url):
        dataset = URIRef(url)
        self.graph.bind("dip", DIPNS)
        id = parse_qs(urlparse(url).query).get('selId')[0]
        self.graph.add((dataset, RDF.type, DCAT.Dataset))
        urlfp = get_dip_with_cookie(url)
        procedure = inline_xml_from_page(urlfp.read())
        urlfp.close()
        if procedure is None: 
            logging.warn("Could not find embedded XML in Ablauf: %s", id)
            return {}
        full_id = procedure.findtext("WAHLPERIODE") + '-' + id
        self.graph.add((dataset, DC.identifier, Literal(full_id)))
        title = procedure.findtext("TITEL")
        self.graph.add((dataset, DC.title, Literal(title)))
        description = procedure.findtext("ABSTRAKT")
        if description:
            self.graph.add((dataset, DC.description, Literal(description)))
        subject = procedure.findtext("SACHGEBIET")
        if subject:
            self.graph.add((dataset, DC.subject, Literal(subject)))
        for keyword in procedure.findall("SCHLAGWORT"):
            self.graph.add((dataset, DC.keyword, Literal(keyword.text)))

        wahlperiode = procedure.findtext("WAHLPERIODE")
        if wahlperiode: 
            self.graph.add((dataset, DIPNS.wahlperiode, Literal(wahlperiode)))
        gesta = procedure.findtext("GESTA_ORDNUNGSNUMMER")
        if gesta: 
            self.graph.add((dataset, DIPNS.gestaOrdnungsnummer, Literal(gesta)))
        stand = procedure.findtext("AKTUELLER_STAND")
        if stand: 
            self.graph.add((dataset, DIPNS.stand, Literal(stand)))
        initiative = procedure.findtext("INITIATIVE")
        if initiative: 
            self.graph.add((dataset, DC.creator, Literal(initiative)))
        vorgangstyp = procedure.findtext("VORGANGSTYP")
        if vorgangstyp: 
            self.graph.add((dataset, DC.type, Literal(vorgangstyp)))
        signature = procedure.findtext("SIGNATUR")
        if signature: 
            self.graph.add((dataset, DIPNS.signatur, Literal(signature)))
        euDokNr = procedure.findtext("EU_DOK_NR")
        if euDokNr: 
            self.graph.add((dataset, DC.references, Literal(euDokNr)))
        zust = procedure.findtext("ZUSTIMMUNGSBEDUERFTIGKEIT")
        if zust: 
            self.graph.add((dataset, DIPNS.zustimmungsbeduerftig, Literal(zust)))
        
        for document in procedure.findall("WICHTIGE_DRUCKSACHE"):
            if not document.findtext("DRS_LINK"):
                continue
            dist = URIRef(document.findtext("DRS_LINK"))
            self.graph.add((dist, RDF.type, DCAT.Distribution))
            self.graph.add((dataset, DCAT.distribution, dist))
            self.graph.add((dist, DCAT.accessURL, dist))
            self.graph.add((dist, DC['format'], Literal("PDF")))
            self.graph.add((dist, DC.type, Literal("Document")))
            drs_id = document.findtext("DRS_NUMMER")
            self.graph.add((dist, DC.identifier, Literal(drs_id)))
            typ = document.findtext("DRS_TYP")
            self.graph.add((dist, RDFS.label, Literal(typ)))
            house = document.findtext("DRS_HERAUSGEBER")
            if house == 'BT':
                bt = URIRef("http://www.bundestag.de")
                self.graph.add((dist, DC.publisher, bt))
                self.graph.add((bt, RDFS.label, Literal("Deutscher Bundestag")))
            if house == 'BR':
                br = URIRef("http://www.bundesrat.de")
                self.graph.add((dist, DC.publisher, br))
                self.graph.add((br, RDFS.label, Literal("Bundesrat")))
        
        for document in procedure.findall("PLENUM"):
            if not document.findtext("PLPR_LINK"):
                continue
            dist = URIRef(document.findtext("PLPR_LINK"))
            self.graph.add((dist, RDF.type, DCAT.Distribution))
            self.graph.add((dataset, DCAT.distribution, dist))
            self.graph.add((dist, DCAT.accessURL, dist))
            self.graph.add((dist, DC['format'], Literal("PDF")))
            self.graph.add((dist, DC.type, Literal("Transcript")))
            drs_id = document.findtext("PLPR_NUMMER")
            self.graph.add((dist, DC.identifier, Literal(drs_id)))
            typ = document.findtext("PLPR_KLARTEXT")
            self.graph.add((dist, RDFS.label, Literal(typ)))
            house = document.findtext("PLPR_HERAUSGEBER")
            if house == 'BT':
                bt = URIRef("http://www.bundestag.de")
                self.graph.add((dataset, DC.publisher, bt))
                self.graph.add((bt, RDFS.label, Literal("Deutscher Bundestag")))
            if house == 'BR':
                br = URIRef("http://www.bundesrat.de")
                self.graph.add((dataset, DC.publisher, br))
                self.graph.add((br, RDFS.label, Literal("Bundesrat")))

        self.activities(dataset, id)
        
        self.write(full_id)




