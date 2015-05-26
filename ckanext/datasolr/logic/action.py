import ckan.logic as logic
import importlib
import pylons

from ckanext.datasolr.lib.database import Connection
from ckanext.datasolr.lib.datastore_solr_search import DatastoreSolrSearch
from ckanext.datasolr.config import config
from ckanext.datastore import db


@logic.side_effect_free
def datastore_solr_search(context, data_dict):
    """ Search a datastore resource using Solr

    This is an alternative to datastore_search, and conforms to the same
    API. See datastore_search.
    """
    resource_config = None
    if data_dict.get('resource_id', None) in config['resources']:
        resource_config = config['resources'][data_dict.get('resource_id', None)]
    elif config['search_url']:
        resource_config = config
    if resource_config:
        sqa_cnx = db._get_engine({
            'connection_url': pylons.config['ckan.datastore.write_url']
        }).connect()
        connection = Connection(sqa_cnx)
        try:
            searcher = DatastoreSolrSearch(
                context, data_dict, resource_config, connection
            )
            searcher.validate()
            return searcher.fetch()
        finally:
            sqa_cnx.close()
    else:
        fallback = config['fallback'].split('.')
        fb_module = importlib.import_module('.'.join(fallback[:-1]))
        func = getattr(fb_module, fallback[-1])
        return func(context, data_dict)