from setuptools import find_packages, setup

__version__ = '1.0.3'

setup(name='ckanext-datasolr',
      version=__version__,
      description='CKAN extension to use Solr to perform datastore queries',
      url='http://github.com/NaturalHistoryMuseum/ckanext-datasolr',
      packages=find_packages(exclude='tests'),
      install_requires=['ujson', 'sqlparse'],
      entry_points='''
        [ckan.plugins]
            datasolr = ckanext.datasolr.plugin:DataSolrPlugin
            '''
      )
