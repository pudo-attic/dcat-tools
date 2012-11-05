import logging
from solr import SolrConnection

def connect(config):
    solr_url = config.get('solr_url', 'http://localhost:8983/')
    solr_user = config.get('solr_user') 
    solr_password = config.get('solr_password')

    return SolrConnection(solr_url, 
                          timeout=500,
                          http_user=solr_user,
                          http_pass=solr_password)

