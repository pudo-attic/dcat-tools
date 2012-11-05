import os
import logging
from pkg_resources import iter_entry_points

from Queue import PriorityQueue
from threading import Thread
from datetime import datetime
from babel import Locale

from dcat.core import *
from dcat.core.processing import ProcessorManager

log = logging.getLogger(__name__)


EP_GROUP = 'crawler.types'

class CrawlManager(object):
    
    def __init__(self, config):
        try: self.queue_size = int(config.get('queue_size'))
        except: self.queue_size = 1000
            
        try: self.num_threads = int(config.get('num_threads'))
        except: self.num_threads = 10
        
        self.queue = PriorityQueue(maxsize=self.queue_size)
        self.processors = ProcessorManager(config)

        for i in  range(self.num_threads):
            t = Thread(target=self.queue_target, 
                       name="CrawlDaemon-%s" % i)
            t.daemon = True
            t.start()
    
    def queue_target(self):
        while True:
            try:
                queue_entry = self.queue.get(True)
                self.handle(queue_entry[1])
                self.queue.task_done()
            except Exception, e:
                log.exception(e)
        
    def handle(self, queue_entry):
        try:
            graph, crawl_class, name, config, args = queue_entry
            crawler = crawl_class(self, name, config)
            if graph is not None:
                crawler.graph += graph
            crawler.run(**args)
        except Exception, e:
            log.exception(e)
    
    def queue_put(self, graph, crawl_class, name, config, 
                  args, low_priority=False):
        if isinstance(crawl_class, basestring):
            crawl_class = self.by_type(crawl_class)
        #log.warn("%s:%s > %r" % (name, crawl_class, args))
        self.queue.put((10 if low_priority else 1, 
                        (graph, crawl_class, name, 
                         config, args)))
        
    def crawl(self):
        self.queue.join()
    
    def by_type(self, type_):
        for ep in iter_entry_points(EP_GROUP, type_):
            return ep.load()
        else:
            raise ValueError("No such crawler type: %s" % type_)
    

class Crawler(object): 
    
    def __init__(self, crawl_manager, name, config):
        self.name = name
        self.crawl_manager = crawl_manager
        self.graph = Graph()
        self.log = logging.getLogger("%s:%s(%s)" % (__name__, self.__class__.__name__, name))
        self.config = self.configure(config)
    
    def queue(self, crawl_class, low_priority=False, **kwargs):
        self.crawl_manager.queue_put(self.graph,
                                     crawl_class, 
                                     self.name, 
                                     self.config, 
                                     kwargs,
                                     low_priority=low_priority)
    
    def expand_dataset(self, dataset):
        if 'publisher_link' in self.config:
            publisher_link = URIRef(self.config.get('publisher_link'))
            self.graph.remove((None, DC.publisher, None))
            self.graph.add((dataset, DC.publisher, publisher_link))
        
        if 'publisher' in self.config:
            for s, p, publisher in self.graph.triples((dataset, DC.publisher, None)):
                self.graph.add((publisher, RDFS.label, 
                    Literal(self.config.get('publisher'))))
        
        if 'locale' in self.config:
            locale = self.config.get('locale').strip()
            #self.graph.add((dataset, DC.language, Literal(locale)))
            _locale = Locale(locale)
            if _locale.display_name:
                self.graph.add((dataset, DC.language, Literal(_locale.display_name)))
            elif '_' in locale:
                lang = locale.split('_',1)[0]
                #self.graph.add((dataset, DC.language, Literal(lang)))
                _lang = Locale(lang)
                if _lang.display_name:
                    self.graph.add((dataset, DC.language, Literal(_lang.display_name)))
            
        if 'country' in self.config:
            self.graph.add((dataset, DC.spatial, Literal(self.config.get('country').strip())))
        
        for processor in self.crawl_manager.processors:
            processor.process(self.graph)

        # This should really go on an artifact or named graph but hey: 
        name = "%s#%s" % (self.__class__.__name__, self.name)
        process = URIRef("http://opendatasearch.org/process/" + name)
        self.graph.add((dataset, OPMV.wasGeneratedBy, process))
        self.graph.add((dataset, OPMV.wasGeneratedBy, process))
    
    def write(self, name):
        for dataset, _, _ in self.graph.triples((None, RDF.type, DCAT.Dataset)):
            self.expand_dataset(dataset)
        crawler_path = os.path.join(self.config.get('out_dir', '.'), self.name)
        if not os.path.isdir(crawler_path):
            os.makedirs(crawler_path)
        fn = os.path.join(crawler_path, "%s-%s.n3" % (self.name, name))
        log.info("Writing %s:%s" % (self.name, name))
        self.graph.serialize(fn, encoding='utf-8', format='n3')
        
        #try:
        #    from dcat.rdfsolr.indexer import index_rdf
        #    from dcat.rdfsolr.solrconn import connect
        #    conn = connect(self.config)
        #    index_rdf(self.graph, conn)
        #except Exception, e:
        #    self.log.exception(e)
        
    
    def run(self):
        raise ValueError("%s: No run method specified!" % self.name)
    
    def configure(self, config):
        return config


class CatalogCrawler(Crawler):
    pass
    
class DatasetCrawler(Crawler):
    pass
    

