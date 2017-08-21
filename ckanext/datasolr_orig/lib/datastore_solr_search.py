import solr
import ckan.plugins as p
import ckanext.datastore.helpers as datastore_helpers
from ckanext.datasolr.logic.schema import datastore_search_schema
import copy

from ckan.lib.navl.dictization_functions import validate
from ckan.plugins import PluginImplementations
from ckanext.datasolr.lib.helpers import parse_sort_statement
from ckanext.datasolr.lib.solrqueryapi import ApiQueryToSolr, SolrQueryToSql
from ckanext.datasolr.interfaces import IDataSolr

from ckanext.datasolr import DQI_FIELDS


class DatastoreSolrSearch(object):
    """ Class used to implement the datastore_solr_search action

    @param context: Ckan execution context
    @param params: Dictionary containing the action parameters
    @param config: Datasolr configuration for this resource
    @param connection: Database connection object
        (ckanext.datasolr.lib.db.Connection)
    """

    def __init__(self, context, params, config, connection):
        self.context = context
        self.config = config
        self.connection = connection
        self.original_params = params
        self.params = params
        self.resource_id = self.params['resource_id']
        self.fields = self.connection.get_fields(self.resource_id)
        self.api_to_solr = ApiQueryToSolr(
            solr_id_field=self.config['solr_id_field'],
            solr_resource_id_field=self.config['solr_resource_id_field'],
            field_mapper=self.config['field_mapper'],
            field_types=self.fields
        )
        self.solr_to_sql = SolrQueryToSql(
            search_url=self.config['search_url'],
            id_field=self.config['id_field'],
            solr_id_field=self.config['solr_id_field']
        )

    def _check_access(self):
        """ Ensure we have access to the defined resource """
        p.toolkit.check_access('datastore_search', self.context, self.params)

    def validate(self):
        """ Validate the query

        Note that as per CKAN validation pipeline, this will both validate and
        convert input parameters. As such it is a necessary step in the process.

        @raises ValidationError: If the request parameters are invalid
        @raises ObjectNotFound: If the resource is not found
        """
        # Validate and process input parameters
        schema = self.context.get('schema', datastore_search_schema())

        schema['solr_stats_fields'] = list(schema['fields'])
        self.params, errors = validate(self.params, schema, self.context)
        if errors:
            raise p.toolkit.ValidationError(errors)
        # Resolve resource id and validate resource existence
        try:
            self.resource_id = self.connection.resolve_alias(
                self.params['resource_id']
            )
        except ValueError:
            raise p.toolkit.ObjectNotFound(p.toolkit._(
                'Resource "{0}" was not found.'.format(self.params['resource_id'])
            ))
        self.params['resource_id'] = self.resource_id
        # Parse & Set default fields if none are present
        if 'fields' in self.params:
            self.params['fields'] = datastore_helpers.get_list(self.params['fields'])
        else:
            self.params['fields'] = self.fields.keys()
        if 'solr_stats_fields' in self.params:
            self.params['solr_stats_fields'] = datastore_helpers.get_list(
                self.params['solr_stats_fields']
            )
        # Parse sort statement
        if 'sort' in self.params:
            self.params['sort'] = parse_sort_statement(self.params['sort'])
        # Validate distinct query (we only accept one field)
        if self.params.get('distinct', False):
            if len(self.params['fields']) != 1:
                raise p.toolkit.ValidationError({
                    'distinct': ['Distinct queries can only have one field']
                })
            self.params['distinct'] = self.params['fields'][0]
        # Now invoke plugins (including the DataSolr plugin) validation
        self.context['api_to_solr'] = self.api_to_solr
        data_dict = copy.deepcopy(self.params)
        for plugin in PluginImplementations(IDataSolr):
            data_dict = plugin.datasolr_validate(
                self.context, data_dict, self.fields
            )
        for key, values in data_dict.items():
            if key in ['resource_id'] or not values:
                continue
            if isinstance(values, basestring):
                value = values
            elif isinstance(values, (list, tuple)):
                value = values[0]
            elif isinstance(values, dict):
                value = values.keys()[0]
            else:
                value = values
            raise p.toolkit.ValidationError({
                key: [u'invalid value "{0}"'.format(value)]
            })

    def fetch(self):
        """ Run the query and fetch the data
        """
        self._check_access()
        solr_args = {
            'q': ([], []),
            'fields': self.params['fields']
        }
        for plugin in PluginImplementations(IDataSolr):
            solr_args = plugin.datasolr_search(
                self.context, self.params, self.fields, solr_args
            )
        self.params['fields'] = solr_args['fields']
        del solr_args['fields']
        results = self.solr_to_sql.fetch(
            resource_id=self.resource_id,
            solr_args=solr_args,
            fields=self.params['fields'],
            sort=self.params.get('sort', None)
        )
        records = self.connection.execute(
            results['sql'], results['values'], row_formatter=self._format_row
        )
        api_field_list = self._get_response_field_list(results)

        # Add additional DQI fields
        for field_name, field_type in DQI_FIELDS.items():
            api_field_list.append({'type': field_type, 'id': field_name})

        response = dict(self.original_params.items() + {
            'fields': api_field_list,
            'total': results['total'],
            'records': records,
            '_backend': 'datasolr'
        }.items())
        if results['next_cursor']:
            response['next_cursor'] = results['next_cursor']
        return response

    def _get_response_field_list(self, results):
        """ Generate the field list to return with the given response

        @param results: Result as returned by solr to sql
        @returns: A list of field definition
        """
        if self.params.get('fields', False):
            api_field_list = []
            for f in self.params['fields']:
                api_field_list.append({'id': f, 'type': self.fields[f]})
        else:
            api_field_list = [{'id': f, 'type': self.fields[f]} for f in self.fields]
        if results['stats'] and 'stats_fields' in results['stats']:
            rev_map = dict([(self.api_to_solr.field_mapper(f), f) for f in self.fields])
            for stat_field in results['stats']['stats_fields']:
                source_field = rev_map[stat_field]
                field_def = {
                    'id': source_field,
                    'type': self.fields[source_field]
                }
                try:
                    index = api_field_list.index(field_def)
                except ValueError:
                    api_field_list.append(field_def)
                    index = len(api_field_list) - 1
                field_list_items = api_field_list[index].items()
                try:
                    field_list_items += results['stats']['stats_fields'][stat_field].items()
                except AttributeError:
                    pass
                api_field_list[index] = dict(field_list_items)
        return api_field_list

    def _format_row(self, row):
        """ Format a row of results

        @param row: A dict like object containing the fields
        """
        result = {}
        for field in self.params['fields']:
            result[field] = self.connection.convert(
                row[field], self.fields[field]
            )
        # Add additional DQI fields
        for field_name in DQI_FIELDS.keys():
            result[field_name] = row[field_name]
        return result