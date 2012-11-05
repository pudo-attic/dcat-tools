import logging, os, string
from urlparse import urlparse
from ckanclient import CkanClient, CkanApiError
from mapper import dcat_to_ckan
from vocab import *
from dcat.cli import DCatCommand

log = logging.getLogger(__name__)

def import_dataset(graph, uri, client):
    data = dcat_to_ckan(graph, uri)
    name = data.get('name')

    source_loc = urlparse(uri).netloc
    dest_loc = urlparse(client.base_location).netloc
    if source_loc != dest_loc:
        name = source_loc + "-" + name
        name = name.replace(".", "_")
        name = "".join([c for c in name if \
            c in "-_" + string.letters + string.digits]).lower()
        data['name'] = name

    if name is None:
        log.error("No 'name' on %s" % uri)
        return
    log.info("Importing %s..." % name)
    try:
        pkg = client.package_entity_get(name)
    except CkanApiError, e:
        client.package_register_post(data)
        return 
    extras = pkg.get('extras', {})
    if extras.get('rdf_source_id') != data.get('extras',
            {}).get('rdf_source_id'):
        log.error('Package name collision: %s, %s' % (name, uri))
        return
    # TODO: Write proper merger.
    pkg.update(data)
    extras.update(data.get('extras', {}))
    pkg['extras'] = extras
    client.package_entity_put(pkg)


def import_graph(graph, config):
    base_location = config.get('import_location', 'http://localhost:5000/api')
    api_key = config.get('import_key')
    client = CkanClient(base_location=base_location, api_key=api_key)
    for ds, _, _ in graph.triples((None, RDF.type, DCAT.Dataset)):
        import_dataset(graph, ds, client)
    for ds, _, _ in graph.triples((None, RDF.type, VOID.Dataset)):
        import_dataset(graph, ds, client)

class ImportCommand(DCatCommand):
    '''Import all files in a given directory to CKAN'''
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        self._load_config()
        if len(self.args):
            source_dir = self.args[0]
        else:
            source_dir = self.config.get('crawl_cache', 'data/crawl_cache')

        if not os.path.isdir(source_dir):
            raise ValueError("Invalid source directory: %s" % source_dir)
        
        for base_dir, _, files in os.walk(source_dir):
            for file_name in files:
                path = os.path.join(base_dir, file_name)
                log.info("Importing: %s" % file_name)
                fh = file(path, 'rb')
                graph = Graph()
                graph.parse(file=fh, format='n3')
                fh.close()
                import_graph(graph, self.config)
