#coding: utf-8
import unittest
import os

from dcat.core import * 
from dcat.core.mapper import dcat_to_ckan

fts = URIRef("http://ckan.net/package/eu-fts")

class DcatToJsonTest(unittest.TestCase):

    def setUp(self):
        fixture_path = os.path.join(os.path.dirname(__file__),
            'mapper_fixtures.n3')
        self.graph = Graph()
        self.graph.parse(fixture_path, format="n3")

    def test_simple_convert(self):
        data = dcat_to_ckan(self.graph, fts)
        assert isinstance(data, dict), data
        assert data.get('name') == "eu-fts", data.get('name')
        assert data.get('title') == "EU - Financial Transparency System", data.get('title')
        assert 'budget' in data.get('tags'), data.get('tags')
        assert 'ec.europa.eu' in data.get('url'), data.get('url')
        assert 'Co-financing rate' in data.get('notes'), data.get('notes')

    def test_simple_convert(self):
        data = dcat_to_ckan(self.graph, fts)
        resources = data['resources']
        res1 = resources[1]
        assert res1['format'] == 'text/xml', res1
        assert 'XML, English' in res1['description'], res1
        assert 'http://storage.ckan.net/wdmmg/fts-' in res1['url'], res1

if __name__ == '__main__':
    unittest.main()

