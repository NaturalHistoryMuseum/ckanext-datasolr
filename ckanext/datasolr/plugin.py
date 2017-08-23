import ckan.plugins as p
from pylons import config
import re

from ckanext.datasolr.interfaces import IDataSolr
from ckanext.datasolr.logic.action import datastore_search
from ckanext.datasolr.lib.helpers import is_datasolr_resource


class DataSolrPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IActions)
    p.implements(p.ITemplateHelpers, inherit=True)
    p.implements(p.IRoutes, inherit=True)
    p.implements(IDataSolr)

    # IActions
    def get_actions(self):
        return {
            'datastore_search': datastore_search
        }

    # ITemplateHelpers
    def get_helpers(self):
        return {
            'is_datasolr_resource': is_datasolr_resource
        }

    # IDataSolr
    def datasolr_validate(self, context, data_dict, fields):
        """ Validates the input request.

        This is the main validator, which will remove all known fields
        from fields, sort, q as well as all other accepted input parameters.
        """
        field_names = [f['id'] for f in fields]
        # Validate field list
        if 'fields' in data_dict:
            data_dict['fields'] = list(
                set(data_dict['fields']) - set(field_names)
            )

        sort = data_dict.get('sort', [])
        # FIXME: Can be an array at this point??
        # Ensure sort is a list
        sort = [sort] if not isinstance(sort, list) else sort
        # Remove all sorts that are valid field names - the remainder
        # Are invalid fields
        data_dict['sort'] = list(set(sort) - set(field_names))

        # Remove all filters that are valid field names
        filters = data_dict.get('filters', {})
        invalid_filter_fields = list(set(filters.keys()) - set(field_names))
        data_dict['filters'] = {k: filters[k] for k in invalid_filter_fields}

        # Remove all facets_field_limit that are valid field names
        facets_field_limit = data_dict.get('facets_field_limit', {})
        data_dict['facets_field_limit'] = list(set(facets_field_limit.keys()) - set(field_names))

        if data_dict.get('q'):
            if isinstance(data_dict['q'], basestring):
                data_dict['q'] = None
            else:
                for field in field_names:
                    if field in data_dict['q']:
                        del data_dict['q'][field]

        # Remove all the known fields
        for field in ['distinct', 'cursor', 'facets', 'facets_limit']:
            data_dict.pop(field, None)

        # Validate offset & limit as integers
        if 'offset' in data_dict:
            try:
                int(data_dict['offset'])
                del data_dict['offset']
            except ValueError:
                pass
        if 'limit' in data_dict:
            try:
                int(data_dict['limit'])
                del data_dict['limit']
            except ValueError:
                pass

        return data_dict

    def datasolr_search(self, context, data_dict, fields, query_dict):
        """ Build the solr search """
        query_params = dict(
            resource_id=data_dict['resource_id'],
            q=data_dict.get('q', []),
            filters=data_dict.get('filters'),
            facets=data_dict.get('facets'),
            facets_limit=data_dict.get('facets_limit'),
            facets_field_limit=data_dict.get('facets_field_limit'),
            limit=data_dict.get('limit', 100),
            distinct=data_dict.get('distinct', False)
        )
        query_params['fields'] = data_dict.get('fields', [f['id'] for f in fields])
        cursor = data_dict.get('cursor', None)
        if cursor:
            # Must be sorted on primary key
            # TODO: We could get the primary field from the solr schema lookup
            query_params['sort'] = [('_id', 'ASC')]
        else:
            # If we've specified a paging cursor, then we don't want to use the offset
            query_params['offset'] = data_dict.get('offset', 0)

        return query_params
