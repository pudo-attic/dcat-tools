import logging
from pprint import pprint
import re
from urllib import urlencode, urlopen
try:
    import json 
except ImportError:
    import simplejson as json

from flask import Flask, request, g, url_for
from flaskext.genshi import Genshi, render_response
from paste.deploy.converters import asbool

from dcat.cli import DCatCommand

import helpers as h
from urllib import urlencode
from solrconn import connect
from page import Page

log = logging.getLogger(__name__)
app = Flask(__name__)

QUERY_PARAMETERS = ['q', 'n', 'p']

class Result(object):
    
    def __init__(self, config, request):
        self.config = config
        self.args = request.args
        self._result = None
        self._facets = None
    
    @property
    def query(self):
        return self.args.get('q', '')
    
    @property
    def has_query(self):
        if len(self.active_facets):
            return True
        return len(self.query)>0
    
    @property
    def filter_query(self):
        fq = []
        if 'filter_publishers' in self.config: 
            publishers = re.split("\s", self.config.get('filter_publishers'))
            publishers = map(lambda p: "+publisher_link:\"%s\"" % p.strip(), publishers)
            if len(publishers): 
                fq.append("(%s)" % " OR ".join(publishers))
        for facet, value in self.active_facets:
            fq.append("+%s:\"%s\"" % (facet, value))
        return fq

    @property
    def page_length(self):
        if not self.has_query:
            return 0
        try:
            return min(100, int(self.args.get('n')))
        except (ValueError, TypeError): 
            return 15

    @property
    def page_num(self):
        try:
            return int(self.args.get('p'))
        except (ValueError, TypeError):
            return 1

    @property
    def result_obj(self):
        if self._result is None:
            query = []
            query.append(('start', max(0, ((self.page_num-1) * self.page_length))))
            query.append(('q', self.query if len(self.query) else '*:*'))
            query.append(('rows', self.page_length))
            query.append(('wt', 'json'))
            query.append(('facet', 'true'))
            query.append(('facet.limit', '6'))
            for fq in self.filter_query:
                query.append(('fq', fq))
            for fa in self.facets.keys():
                query.append(('facet.field', fa))
            query = [(k, unicode(q).encode('utf-8')) for k, q in query]
            query = urlencode(query, doseq=True)

            # TODO: Support HTTP auth or use solrpy after all. 
            # solrpy was dropped due to urlencoding issues. 
            url = self.config.get('solr_url', 'http://localhost:8983/')
            url += '/select/?' + query
            urlfh = urlopen(url)
            self._result = json.loads(urlfh.read())
            urlfh.close()
        return self._result

    @property
    def results(self):
        return self.result_obj.get('response', {})\
            .get('docs', [])
                
    @property
    def result_count(self):
        return self.result_obj.get('response', {})\
            .get('numFound', 0)

    @property
    def facets(self):
        prefix = 'facet.'
        if self._facets is None:
            self._facets = {}
            for (k, v) in self.config.items():
                if k.startswith(prefix):
                    name = k.split(prefix,1)[-1]
                    self._facets[name] = v
        return self._facets

    def facet_options(self, name):
        _facets = self.result_obj.get('facet_counts', {}).get('facet_fields', {})
        for facet, values in _facets.items():
            if facet != name:
                continue
            for value in values[::2]:
                count =  values[values.index(value)+1]
                if count > 0 and not (facet, value) in self.active_facets:
                    yield (value, count)
    
    @property
    def form_fields(self):
        fields = []
        for field in self.args.keys():
            if not field in ['q', 'p', 'submit']:
                fields.extend([(field, v) for v in \
                    self.args.getlist(field)])
        return fields

    @property
    def query_params(self):
        return self.form_fields + [('q', self.query)]
    
    @property
    def active_facets(self):
        return [(k, v) for k, v in self.form_fields\
            if not k in QUERY_PARAMETERS]

    def _make_url(self, params):
        _params = []
        for (k, v) in params:
            if isinstance(v, unicode):
                v = v.encode('utf-8')
            if isinstance(v, basestring) and not len(v):
                continue
            _params.append((k, v))
        return "/?" + urlencode(_params)
    
    def add_filter_url(self, name, value):
        return self._make_url([(n,v) for n,v in self.query_params if not n=='p'] \
            + [(name, value)])

    def remove_filter_url(self, name, value):
        return self._make_url([(n,v) for n,v in self.query_params\
            if n=='p' or not (n==name and v==value)])
    
    def page_url(self, page, partial=None):
        return self._make_url([(n,v) for n,v in self.query_params\
            if not n=='p'] + [('p', page)])
    
    def pager(self):
        return Page(self.result_obj, page=self.page_num, url=self.page_url).pager()

    def __len__(self):
        return self.result_count


@app.before_request
def before_request():
    """Make sure we are connected to Solr for each request."""
    g.config = app.my_config

@app.after_request
def after_request(response):
    """Closes the connection again at the end of the request."""
    return response

@app.route("/opensearch.xml")
def index():
    tpl = {'app': app, 'h': h}
    return render_response('opensearch.xml', context=tpl)

@app.route("/")
def index():
    tpl = {'app': app, 'h': h}
    tpl['result'] = Result(g.config, request)
    return render_response('index.html', context=tpl)

def configure_app(config):
    app.my_config = config
    app.site_title = config.get('site_title', 'DCat Search')
    app.site_slogan = config.get('site_slogan', 'data catalogue indexer')
    app.site_logo = config.get('site_logo', 'media/logo.png')
    app.site_url = config.get('site_title')
    app.site_style = config.get('site_style')
    genshi = Genshi(app)
    genshi.extensions['html'] = 'html5'
    app.debug = asbool(config.get('debug', 'false'))
    return app

class RunCommand(DCatCommand):
    '''Run the web UI'''
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self, run=True):
        self._load_config()
        app = configure_app(self.config)
        if run:
            app.run()
        else:
            return app


def wsgi(config_file):
    command = RunCommand(__name__)
    class _opt(object):
        config = config_file
    command.options = _opt()
    command.options.config = config_file
    return command.command(run=False)

