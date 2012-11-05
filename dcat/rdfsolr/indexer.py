import logging, os
from pprint import pprint
from dateutil import tz
from datetime import datetime

from dcat.core import *
from dcat.cli import DCatCommand

from mappings import FILTERS, MAPPING
from solrconn import connect

log = logging.getLogger(__name__)

def flat_graph(graph, obj, path=None):
    """ Flatten the graph, seen from obj to result in a list of tuples,
        each with a tuple of all traversed nodes and the value. 
    """
    if path is None:
        path = [obj]
        yield ((DC.source,), obj)
    for _, p, o in graph.triples((obj, None, None)):
        p = (p,)
        if p == OWL.sameAs:
            for k, v in flat_graph(graph, o, path=path):
                yield (k, v)
            continue
        elif isinstance(o, (URIRef, BNode)):
            if o in path: 
                continue
            if isinstance(o, URIRef):
                yield (p, o)
            path.append(o)
            for k, v in flat_graph(graph, o, path=path):
                yield (p + k, v)
        else:
            yield (p, o)

def convert_values(items):
    """ Convert RDF Literals to Python values. """
    for k, v in items:
        if isinstance(v, Literal):
            if v.language:
                yield ('lang', v.language)
            if v.datatype:
                v = v.toPython()
                if isinstance(v, datetime) and not v.tzinfo:
                    v = datetime(v.year, v.month, v.day, v.hour, 
                                 v.minute, v.second, tzinfo=tz.tzutc())
                yield (k, v)
                continue
        yield (k, unicode(v))

def index_map(graph, items, mapping=MAPPING):
    """ Select index-friendly names for all items, defaulting to 
        the qname suffix. 
    """
    for k, v in items:
        if k in mapping:
            yield (mapping.get(k), v)
        elif len(k)==1 and k[0] in mapping:
            yield (mapping.get(k[0]), v)
        else:
            try:
                k = [graph.qname(p).split(':', 1)[-1] for p in k]
            except TypeError, e:
                # TODO: Handle non-existing ontologies!
                log.exception(e)
            yield (".".join(k), v)


def index_rdf(graph, conn):
    """ Expects something that is roughly DCat-shaped. """
    
    # filter out unwanted edges
    for filter_ in FILTERS:
        graph.remove(filter_)
    
    entries = []
    for dataset, _, _ in graph.triples((None, RDF.type, DCAT.Dataset)):
        items = convert_values(flat_graph(graph, dataset))
        entry = {'id': hash(dataset)}
        for k, v in index_map(graph, items):
            if not k in entry:
                entry[k] = v
            elif isinstance(entry[k], list):
                entry[k] = list(set(entry[k] + [v]))
            else:
                entry[k] = list(set([entry[k], v]))
        
        #pseudo field for unwanted stuff
        if '_drop' in entry:
            del entry['_drop']
            
        #unique fields:
        for field, values in entry.items():
            if field not in ['title', 'link', 'type', 'name', 'description',
                             'publisher', 'publisher_link', 'license']:
                continue
            if isinstance(values, (tuple, list)):
                entry[field] = values[0]
                log.warn("Dropping additional metadata from %s: %s" % (field, values))
        
        entry['link'] = unicode(dataset)
        e = {}
        for k, v in entry.items(): 
            e[str(k)] = v
        entries.append(e)
    conn.add_many(entries)
    conn.commit()


class IndexCommand(DCatCommand):
    '''Indexes all files in a given directory'''
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        self._load_config()
        conn = connect(self.config)

        if len(self.args):
            out_dir = self.args[0]
        else:
            out_dir = self.config.get('crawl_cache', 'data/crawl_cache')

        if not os.path.isdir(out_dir):
            raise ValueError("Invalid source directory: %s" % out_dir)
        
        for base_dir, _, files in os.walk(out_dir):
            for file_name in files:
                path = os.path.join(base_dir, file_name)
                log.info("Indexing: %s" % file_name)
                fh = file(path, 'rb')
                graph = Graph()
                graph.parse(file=fh, format='n3')
                fh.close()
                index_rdf(graph, conn)
            conn.optimize()
            conn.commit()
