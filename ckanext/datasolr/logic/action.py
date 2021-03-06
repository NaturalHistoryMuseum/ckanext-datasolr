#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-datasolr
# Created by the Natural History Museum in London, UK

from ckanext.datasolr.lib.helpers import is_datasolr_resource
from ckanext.datasolr.lib.solr_search import SolrSearch

from ckan.plugins import toolkit
import ckan.logic as logic


@logic.side_effect_free
@toolkit.chained_action
def datastore_search(prev_func, context, data_dict):
    '''Overrides the default datastore search.
    
    The datastore_search action allows you to search data in a resource.
    DataStore resources that belong to private CKAN resource can only be
    read by you if you have access to the CKAN resource and send the appropriate
    authorization.

    :param resource_id: id or alias of the resource to be searched against
    :type resource_id: string
    :param filters: matching conditions to select, e.g {"key1": "a", "key2": "b"} (optional)
    :type filters: dictionary
    :param q: full text query. If it's a string, it'll search on all fields on
              each row. If it's a dictionary as {"key1": "a", "key2": "b"},
              it'll search on each specific field (optional)
    :type q: string or dictionary
    :param distinct: return only distinct rows (optional, default: false)
    :type distinct: bool
    :param plain: treat as plain text query (optional, default: true)
    :type plain: bool
    :param language: language of the full text query (optional, default: english)
    :type language: string
    :param limit: maximum number of rows to return (optional, default: 100)
    :type limit: int
    :param offset: offset this number of rows (optional)
    :type offset: int
    :param fields: fields to return (optional, default: all fields in original order)
    :type fields: list or comma separated string
    :param sort: comma separated field names with ordering
                 e.g.: "fieldname1, fieldname2 desc"
    :param count: If True, the result will include a 'total' field
                  to the total number of matching rows. (optional, default: True)
    :param fields: fields/columns and their extra metadata
    :type fields: list of dictionaries
    :param offset: query offset value
    :type offset: int
    :param limit: query limit value
    :type limit: int
    :param filters: query filters
    :type filters: list of dictionaries
    :param total: number of total matching records
    :type total: int
    :param records: list of matching results
    :type records: list of dictionaries
    :param context: param data_dict:
    :param data_dict: 

    '''

    resource_id = data_dict.get(u'resource_id')

    # If this isn't a datasolr resource (we've hijacked all datastore searches at this
    # point), reroute request to the real datastore search endpoint
    if not is_datasolr_resource(resource_id):
        # Remove the indexed only flag
        data_dict.pop(u'indexed_only', None)
        # Pass request to the original datastore search
        return prev_func(context, data_dict)

    solr_search = SolrSearch(resource_id, context, data_dict)
    solr_search.validate()
    return solr_search.fetch()
