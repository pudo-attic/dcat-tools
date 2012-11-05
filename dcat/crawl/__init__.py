import os
from ConfigParser import SafeConfigParser
from paste.deploy.converters import asbool

CONFIX_PREFIX = 'crawler:'

from dcat.cli import DCatCommand

# Interesting catalogues:
#   * UN 
#   * Opengov.se
#   * Data Publica
#   * NationalDataCatalog
#   * LondonGovUk
#   * digitaliser.dk

def run_crawlers(global_config, config_file, out_dir):
    from base import CrawlManager
    config = SafeConfigParser() 
    config.read([config_file])
    crawl_manager = CrawlManager(global_config)
    for section in config.sections():
        
        if not section.startswith(CONFIX_PREFIX):
            continue
        _, crawler = section.split(':', 1)
        
        crawler_config = dict(config.items(section))
        assert 'type' in crawler_config, "No 'type' for crawler: %s" % crawler
        
        if asbool(crawler_config.get('skip', 'False')):
            continue
        
        crawler_config['out_dir'] = out_dir
        type_ = crawler_config.get('type')
        crawl_manager.queue_put(None, type_, crawler, 
                                crawler_config, {}, low_priority=True)
    crawl_manager.crawl()
    
    
class CrawlCommand(DCatCommand):
    '''Crawls known metadata sources'''
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 0
    min_args = 0

    def command(self):
        self._load_config()
            
        out_dir = self.config.get('crawl_cache', 'data/crawl_cache')
        if not os.path.isdir(out_dir):
            os.makedirs(out_dir)
        run_crawlers(self.config, self.filename, out_dir)
