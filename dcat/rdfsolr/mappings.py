from dcat.core import *

__all__ = ['FILTERS', 'MAPPING']

FILTERS = [
    (None, FOAF.mbox, None),
    (None, OPMV.wasGeneratedBy, None)
]
 
MAPPING = {
    DC.identifier: 'name',
    DC.title: 'title',
    DC.description: 'description',
    DC.rights: 'rights',
    (DC.rights, RDFS.label): 'rights_label',
    DC.format: 'format',
    RDF.type: 'type',
    RDFS.label: 'label',
    FOAF.page: 'url',
    FOAF.homepage: 'url',
    DCAT.keyword: 'tag',
    DC.publisher: 'publisher_link',
    (DC.publisher, RDFS.label): 'publisher',
    DC.spatial: 'spatial',
    (DC.spatial, RDFS.label): 'spatial',
    DC.language: 'lang',
    DC.source: 'label',
    (FOAF.page, FOAF.primaryTopic, DC.title): 'tag',
    (FOAF.page, FOAF.primaryTopic, DC.publisher): 'publisher_link',
    (FOAF.page, FOAF.primaryTopic, DC.publisher, RDFS.label): 'publisher',
    (FOAF.page, FOAF.primaryTopic, RDFS.label): 'tag',
    (DC.creator, FOAF.name): 'creator',
    (DC.creator, FOAF.name): 'creator',
    (DC.creator, RDFS.label): 'creator',
    (DC.creator, DC.title): 'creator',
    (DC.license, RDFS.label): 'license',
    (DCAT.creator, DC.title): 'creator',
    (DC.creator, FOAF.name): 'creator',
    (DC.relation, RDFS.label): 'label',
    (DC.contributor, FOAF.name): 'creator',
    (DCAT.distribution, RDFS.label): 'label',
    (DCAT.distribution, DC['format']): 'format',
    (DCAT.distribution, DC['format'], RDFS.label): 'format',
    (DCAT.distribution, DC['format'], DC.title): 'label',
    #(DCAT.distribution, DC['format'], RDFS.value): 'format',
    (DCAT.distribution, DCAT.accessURL): 'url',
    (DCAT.distribution, RDF.type): '_drop',
    (FOAF.page, FOAF.primaryTopic): '_drop',
    (FOAF.page, RDF.type): '_drop',
    (FOAF.page, FOAF.primaryTopic, RDF.type): '_drop',
    (FOAF.page, FOAF.primaryTopic, FOAF.isPrimaryTopicOf): '_drop',
    (OWL.sameAs): '_drop',
}
