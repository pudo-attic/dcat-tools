import sys
from dcat.core import *

EXTRAS_MAPPING = {
    'Source/Citation': DC.source,
    'Acronym': DCAT.keyword,
    'Number of economies': DC.extent,
    'Update frequency': DC.accrualPeriodicity,
    'Periodicity': DC.accrualPeriodicity,
    'Update schedule': DC.accrualPolicy,
    'Next expected date of update': DC.accrualPolicy,
    'Type': DC.subject,
    'Granularity': DCAT.granularity,
    'Last update': DC.modified,
    'Data notes': RDFS.label,
    'Coverage': DC.spatial
}

TYPES = {
    'EXCEL': 'application/vnd.ms-excel',
    '(Excel)': 'application/vnd.ms-excel',
    '(CSV)': 'text/csv',
    'Available in the API': 'api/json',
    'Databank': 'api/html'
}

#id=ctl00_ContentPlaceHolderDefault_ResourceViewUC_lblResponsibleUserName/a
#ctl00_ContentPlaceHolderDefault_ResourceViewUC_lblPubliced
#ctl00_ContentPlaceHolderDefault_ResourceViewUC_lblType
#ctl00_ContentPlaceHolderDefault_ResourceViewUC_divResourceDescription
#ctl00_ContentPlaceHolderDefault_ResourceViewUC_ucTags_tagsList_htmlUL
#ctl00_ContentPlaceHolderDefault_ResourceViewUC_supplemetaryTabContainer
#  dl
#    dt
#    dd
#  id=dlTaxonomynodes, dl
#    dt
#     dd
#      dl
#       dt
#       dd
#<table summary="Filer og referencer tilknyttet ressourcen">
#  tr class=odd|even|odd-last|even-last


class DigitaliserDkDatasetCrawler(Crawler):

    def handle_doc(self, doc, url=None):
        dataset = URIRef(url)
        log.warn(doc)
        #self.graph.add((dataset, RDF.type, DCAT.Dataset))
        #rights = URIRef('http://data.worldbank.org/summary-terms-of-use')
        #self.graph.add((dataset, DC.rights, rights))
        #self.graph.add((rights, RDFS.label, Literal("Attribution Terms")))
        #self.graph.add((dataset, DC.creator, Literal("The World Bank")))

        #name = url.rsplit('/', 1)[-1].strip()
        #self.graph.add((dataset, DC.identifier, Literal(name)))
        #title = doc.findtext('//h2[@class="page-title "]').strip()
        #self.graph.add((dataset, DC.title, Literal(title)))
        #description = doc.findtext('//div[@class="node-body"]/p')
        #self.graph.add((dataset, DC.description, Literal(description)))

        #time = BNode()
        #self.graph.add((time, RDF.type, TIME.Interval))
        #self.graph.add((dataset, DC.temporal, time))

        #for extra_rows in doc.findall('//div[@class="view-content"]/div/div'):
        #    key = extra_rows.findtext('label').strip()
        #    value = Literal(extra_rows.findtext('span').strip())
        #    if key == 'Start date':
        #        self.graph.add((time, TIME.start, Literal(value)))
        #        continue
        #    if key == 'End date':
        #        self.graph.add((time, TIME.end, Literal(value)))
        #        continue
        #    pred = EXTRAS_MAPPING.get(key)
        #    if pred is None:
        #        log.warn("Unknwon extra field: %s" % key)
        #    else:
        #        self.graph.add((dataset, pred, Literal(value)))
        #        self.graph.add((Literal(value), RDFS.comment, Literal(key)))

        #for resource in doc.findall('//div[@class="views-field-nothing"]//a'):
        #    res = BNode()
        #    self.graph.add((res, RDF.type, DCAT.Distribution))
        #    self.graph.add((dataset, DCAT.distribution, res))
        #    res_label = resource.xpath('string()').strip()
        #    for pattern, mime in TYPES.items():
        #        if pattern in res_label:
        #            self.graph.add((res, DC['format'], Literal(mime)))
        #    self.graph.add((res, RDFS.label, Literal(res_label)))
        #    self.graph.add((res, DC.title, Literal(res_label)))
        #    accessURL = URIRef(resource.get('href').strip())
        #    self.graph.add((res, DCAT.accessURL, accessURL))

        #for related in doc.findall('//div[@class="views-field-field-sidebar-value"]//a'):
        #    rel_label = Literal(related.xpath('string()').strip())
        #    accessURL = URIRef(related.get('href').strip())
        #    self.graph.add((dataset, DC.relation, accessURL))
        #    self.graph.add((accessURL, RDFS.label, rel_label))

        #self.write(name)
