from setuptools import setup, find_packages
import sys, os

from dcat import __version__

setup(name='dcat-tools',
      version=__version__,
      description="DCat-Catalog toolkit",
      long_description=""" """,
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='dcat rdf data catalogue catalog linkeddata',
      author='Open Knowledge Foundation',
      author_email='info@okfn.org',
      url='http://www.okfn.org',
      license='GPLv3',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          # -*- Extra requirements: -*-
          "Flask>=0.6.1",
          "Flask-Genshi>=0.5.1",
          "rdflib>=3.0.0",
          "solrpy>=0.9.3",
          "PasteDeploy>=1.3.4",
          "Babel>=0.9.5",
          "CkanClient>=0.6"
      ],
      entry_points="""
      # -*- Entry points: -*-
      
      [rdf.process]
      formats = dcat.core.normalize:FormatsGraphProcessor

      [paste.paster_command]
      index = dcat.rdfsolr:IndexCommand    
      crawl = dcat.crawl:CrawlCommand    
      run = dcat.rdfsolr.search:RunCommand    
      import = dcat.core.ckan:ImportCommand
      
      [crawler.types]
      ckan = dcat.crawl.ckan:CkanCatalogCrawler
      xml_index = dcat.crawl.xml:XMLCatalogCrawler
      html_index = dcat.crawl.xml:HTMLCatalogCrawler
      sunlight = dcat.crawl.nationaldatacatalog:NationalDataCatalogCrawler
      worldbank = dcat.crawl.worldbank:WorldBankDatasetCrawler
      opengov_se = dcat.crawl.opengov_se:OpengovSeCatalogCrawler
      data_gov_au = dcat.crawl.australia:DataGovAuCatalogCrawler
      data_publica = dcat.crawl.data_publica:DataPublicaCatalogCrawler
      juris = dcat.crawl.juris:JurisCatalogCrawler
      dip21 = dcat.crawl.dip21:DIP21CatalogCrawler
      genesis = dcat.crawl.destatis:GenesisCatalogCrawler
      csw = dcat.crawl.csw:CSWCatalogCrawler
      
      """,
      )
