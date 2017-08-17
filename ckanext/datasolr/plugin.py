import ckan.plugins as p
from pylons import config
import re


from ckanext.datasolr.interfaces import IDataSolr
from ckanext.datasolr.logic.action import datastore_search


class DataSolrPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IActions)
    p.implements(p.IRoutes, inherit=True)
    p.implements(IDataSolr)

    # IActions
    def get_actions(self):
        return {
            'datastore_search': datastore_search
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

        invalid_sort = []
        sort = data_dict.get('sort', None)
        # FIXME: Can be an array at this point??
        if sort:
            # If sort field does not exist in the field list
            # Add it back to the sort as invalid to fail
            if sort not in field_names:
                invalid_sort.append(sort)
        data_dict['sort'] = invalid_sort

        print('FILTERSSS')
        print(data_dict.get('filters', {}))

        # Remove all the known fields
        for field in ['q', 'filters', 'distinct', 'cursor', 'facets']:
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
            q=data_dict.get('q', None),
            filters=data_dict.get('filters', None),
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