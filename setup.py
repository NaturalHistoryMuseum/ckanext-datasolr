from setuptools import setup, find_packages

with open('ckanext/datasolr/version.py') as f:
    exec(f.read())

setup(
    name='ckanext-datasolr',
    version=__version__,
    description='Ckan extension to use Solr to perform datastore queries',
    url='http://github.com/NaturalHistoryMuseum/ckanext-datasolr',
    packages=find_packages(exclude='tests'),
	entry_points="""
        [ckan.plugins]
            datasolr = ckanext.datasolr.plugin:DataSolrPlugin
	"""
)
