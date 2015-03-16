import ckan.logic as logic
import pylons

from ckanext.datasolr.lib.database import Connection
from ckanext.datasolr.lib.datastore_solr_search import DatastoreSolrSearch
from ckanext.datastore import db


@logic.side_effect_free
def datastore_solr_search(context, data_dict):
    """ Search a datastore resource using Solr

    This is an alternative to datastore_search, and conforms to the same
    API. See datastore_search.
    """
    sqa_cnx = db._get_engine({
        'connection_url': pylons.config['ckan.datastore.write_url']
    }).connect()
    connection = Connection(sqa_cnx)
    try:
        searcher = DatastoreSolrSearch(context, data_dict, connection)
        return searcher.fetch()
    finally:
        sqa_cnx.close()