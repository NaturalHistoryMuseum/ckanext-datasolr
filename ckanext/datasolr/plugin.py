#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-datasolr
# Created by the Natural History Museum in London, UK

import re
from ckanext.datasolr.interfaces import IDataSolr
from ckanext.datasolr.lib.helpers import is_datasolr_resource
from ckanext.datasolr.logic.action import datastore_search

from ckan.plugins import interfaces, SingletonPlugin, implements


class DataSolrPlugin(SingletonPlugin):
    ''' '''
    implements(interfaces.IActions)
    implements(interfaces.ITemplateHelpers, inherit=True)
    implements(IDataSolr)

    # IActions
    def get_actions(self):
        return {
            u'datastore_search': datastore_search
            }

    # ITemplateHelpers
    def get_helpers(self):
        return {
            u'is_datasolr_resource': is_datasolr_resource
            }

    # IDataSolr
    def datasolr_validate(self, context, data_dict, fields):
        '''Validates the input request.
        
        This is the main validator, which will remove all known fields
        from fields, sort, q as well as all other accepted input parameters.

        :param context:
        :param fields: 
        :param data_dict: 

        '''
        field_names = [f[u'id'] for f in fields]
        # Validate field list
        if u'fields' in data_dict:
            data_dict[u'fields'] = list(set(data_dict[u'fields']) - set(field_names))

        sort = data_dict.get(u'sort', [])
        # Ensure sort is a list
        sort = [sort] if not isinstance(sort, list) else sort
        # Before validating field name, replace asc/desc from
        sort = [re.sub(u'\s(desc|asc)', u'', s) for s in sort]
        # Remove all sorts that are valid field names - the remainder
        # Are invalid fields
        data_dict[u'sort'] = list(set(sort) - set(field_names))
        # Remove all filters that are valid field names
        filters = data_dict.get(u'filters', {})
        invalid_filter_fields = list(set(filters.keys()) - set(field_names))
        data_dict[u'filters'] = {k: filters[k] for k in invalid_filter_fields}

        # Remove all facets_field_limit that are valid field names
        facets_field_limit = data_dict.get(u'facets_field_limit', {})
        data_dict[u'facets_field_limit'] = list(
            set(facets_field_limit.keys()) - set(field_names))

        if data_dict.get(u'q'):
            if isinstance(data_dict[u'q'], basestring):
                data_dict[u'q'] = None
            else:
                for field in field_names:
                    if field in data_dict[u'q']:
                        del data_dict[u'q'][field]

        # Remove all the known fields
        for field in [u'distinct', u'cursor', u'facets', u'facets_limit',
                      u'indexed_only']:
            data_dict.pop(field, None)

        # Validate offset & limit as integers
        if u'offset' in data_dict:
            try:
                int(data_dict[u'offset'])
                del data_dict[u'offset']
            except ValueError:
                pass
        if u'limit' in data_dict:
            try:
                int(data_dict[u'limit'])
                del data_dict[u'limit']
            except ValueError:
                pass

        return data_dict

    def datasolr_search(self, context, data_dict, fields, query_dict):
        '''Build the solr search

        :param context:
        :param fields:
        :param data_dict: 
        :param query_dict: 

        '''
        query_params = dict(resource_id=data_dict[u'resource_id'],
                            q=data_dict.get(u'q', []), filters=data_dict.get(u'filters'),
                            facets=data_dict.get(u'facets'),
                            facets_limit=data_dict.get(u'facets_limit'),
                            facets_field_limit=data_dict.get(u'facets_field_limit'),
                            limit=data_dict.get(u'limit', 100),
                            sort=data_dict.get(u'sort'),
                            distinct=data_dict.get(u'distinct', False))
        query_params[u'fields'] = data_dict.get(u'fields', [f[u'id'] for f in fields])
        cursor = data_dict.get(u'cursor', None)
        if cursor:
            # Must be sorted on primary key
            # TODO: We could get the primary field from the solr schema lookup
            query_params[u'sort'] = [(u'_id', u'ASC')]
        else:
            # If we've specified a paging cursor, then we don't want to use the offset
            query_params[u'offset'] = data_dict.get(u'offset', 0)

        return query_params
