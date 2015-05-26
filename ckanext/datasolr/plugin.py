import ckan.plugins as p
from pylons import config as pylons_config
import re

from ckanext.datasolr.config import config
from ckanext.datasolr.interfaces import IDataSolr
from ckanext.datasolr.logic.action import datastore_solr_search


SOLR_NOT_EMPTY_FILTER = '_solr_not_empty'


class DataSolrException(Exception):
    pass


class DataSolrPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IConfigurable)
    p.implements(p.interfaces.IActions)
    p.implements(p.IRoutes, inherit=True)
    p.implements(IDataSolr)

    def __new__(cls, *args, **kwargs):
        """ Ensure we are the first IDataSolr plugin """
        idatasolr_extensions = p.PluginImplementations(IDataSolr)
        idatasolr_extensions = idatasolr_extensions.extensions()

        if idatasolr_extensions and idatasolr_extensions[0].__class__ != cls:
            msg = ('The "datasolr" plugin must be the first IDataSolr '
                   'plugin loaded. Change the order it is loaded in '
                   '"ckan.plugins" in your CKAN .ini file and try again.')
            raise  DataSolrException(msg)

        return super(cls, cls).__new__(cls, *args, **kwargs)

    # IConfigurable
    def configure(self, ckan_config):
        prefix = 'datasolr.'
        config_items = ['replace_datastore_search', 'fallback',
                        'search_url', 'id_field', 'solr_id_field',
                        'solr_resource_id_field']
        for long_name in ckan_config:
            if not long_name.startswith(prefix):
                continue
            name = long_name[len(prefix):]
            if name in config_items:
                config[name] = ckan_config[long_name]
            elif name.startswith('resource.'):
                r = re.search('^resource\.([-a-fA-F0-9]+)\.([^.]+)$', name)
                if not r:
                    continue
                resource_id = r.group(1)
                setting = r.group(2)
                value = ckan_config[long_name]
                if resource_id not in config['resources']:
                    config['resources'][resource_id] = config['resources']['_defaults']
                config['resources'][resource_id][setting] = value

    # IActions
    def get_actions(self):
        return {
            'datastore_solr_search': datastore_solr_search
        }

    # IRoutes
    def before_map(self, map):
        # configure hasn't been invoked yet, so read off the pylons config.
        replace = pylons_config.get('datasolr.replace_datastore_search', 'False')
        if replace.lower() in ['yes', 'ok', 'true', '1']:
            map.connect(
                'datasolr',
                '/api/3/action/datastore_search',
                controller='api',
                action='action',
                logic_function='datastore_solr_search',
                ver=u'/3'
            )
            map.connect(
                'datasolr',
                '/api/action/datastore_search',
                controller='api',
                action='action',
                logic_function='datastore_solr_search',
                # ver=u'/3'
            )
        return map

    # IDataSolr
    def datasolr_validate(self, context, data_dict, field_types):
        """ Validates the input request.

        This is the main validator, which will remove all known fields
        from fields, sort, q as well as all other accepted input parameters.
        """
        # Validate field list
        if 'fields' in data_dict:
            data_dict['fields'] = list(
                set(data_dict['fields']) - set(field_types.keys())
            )
        if 'solr_stats_fields' in data_dict:
            data_dict['solr_stats_fields'] = list(
                set(data_dict['solr_stats_fields']) - set(field_types.keys())
            )
        # Validate sort
        val_sort = []
        for field, sort_order in data_dict.get('sort', []):
            if field not in field_types:
                val_sort.append((field, sort_order))
        data_dict['sort'] = val_sort
        # Validate q/filters using api_to_solr
        q, filters = context['api_to_solr'].validate(
            data_dict.get('q', None),
            data_dict.get('filters', {})
        )
        if '_solr_not_empty' in filters:
            del filters['_solr_not_empty']
        if q:
            data_dict['q'] = q
        elif 'q' in data_dict:
            del data_dict['q']
        if filters:
            data_dict['filters'] = filters
        elif 'filters' in data_dict:
            del data_dict['filters']
        # Validate distinct query
        if 'distinct' in data_dict:
            del data_dict['distinct']
        # Validate cursor query
        if 'cursor' in data_dict:
            del data_dict['cursor']
        # Validate offset & limit
        if 'offset' in data_dict:
            try:
                v = int(data_dict['offset'])
                del data_dict['offset']
            except ValueError:
                pass
        if 'limit' in data_dict:
            try:
                v = int(data_dict['limit'])
                del data_dict['limit']
            except ValueError:
                pass
        return data_dict

    def datasolr_search(self, context, data_dict, field_types, query_dict):
        """ Build the solr search """
        api_to_solr = context['api_to_solr']

        query_params = {
            'resource_id': data_dict['resource_id'],
            'q': data_dict.get('q', None),
            'filters': data_dict.get('filters', None),
            'limit': data_dict.get('limit', 100),
            'distinct': data_dict.get('distinct', False)
        }
        cursor = data_dict.get('cursor', None)
        if cursor:
            # Must be sorted on a primary key
            # FIXME: This shouldn't be hard coded? How can we find the primary ID?
            query_params['sort'] = [('occurrenceID', 'ASC')]
        else:
             # If we've specified a paging cursor, then we don't want to use the offset
            query_params['offset'] = data_dict.get('offset', 0)

        solr_args = api_to_solr.build_query(**query_params)

        if cursor:
            solr_args['cursorMark'] = cursor

        query_dict = dict(query_dict.items() + solr_args.items())
        # Add field statistics
        if 'solr_stats_fields' in data_dict:
            query_dict['stats'] = 'true'
            query_dict['stats.field'] = [
                api_to_solr.field_mapper(f) for f in data_dict['solr_stats_fields']
            ]
        # Add 'not empty' query
        if 'filters' in data_dict and SOLR_NOT_EMPTY_FILTER in data_dict['filters']:
            for field in data_dict['filters'][SOLR_NOT_EMPTY_FILTER]:
                query_dict['q'][0].append('{}:[* TO *]'.format(api_to_solr.field_mapper(field)))
        return query_dict
