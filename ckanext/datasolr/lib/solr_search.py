import copy
import ckan.plugins as p
from ckan.plugins import PluginImplementations
from ckan.lib.navl.dictization_functions import validate
import ckanext.datastore.helpers as datastore_helpers

from ckanext.datasolr.lib.solr_connection import SolrConnection
from ckanext.datasolr.lib.config import get_datasolr_resources
from ckanext.datasolr.logic.schema import datastore_search_schema
from ckanext.datasolr.interfaces import IDataSolr
from ckanext.datasolr.lib.helpers import split_words


class SolrSearch(object):
    """ Class used to implement the solr search action
    @param context: Ckan execution context
    @param context: Ckan execution context
    @param params: Dictionary containing the action parameters
    """

    def __init__(self, resource_id, context, params):
        self.context = context
        self.params = params
        self.resource_id = resource_id
        datasolr_resources = get_datasolr_resources()
        self.conn = SolrConnection(datasolr_resources[resource_id])
        # Flag to denote whether to only return fields which have been indexed
        # Used when we need to provide a list of filters
        self.indexed_only = params.get('indexed_only', False)
        self.indexed_fields = self.conn.indexed_fields()
        self.stored_fields = self.conn.stored_fields()

    def _check_access(self):
        """ Ensure we have access to the defined resource """
        p.toolkit.check_access('datastore_search', self.context, self.params)

    def validate(self):
        schema = self.context.get('schema', datastore_search_schema())
        self.params, errors = validate(self.params, schema, self.context)
        if errors:
            raise p.toolkit.ValidationError(errors)
        self.params['resource_id'] = self.resource_id
        # Parse & Set default fields if none are present
        if 'fields' in self.params:
            self.params['fields'] = datastore_helpers.get_list(self.params['fields'])

        data_dict = copy.deepcopy(self.params)
        for plugin in PluginImplementations(IDataSolr):
            data_dict = plugin.datasolr_validate(
                self.context, data_dict, self.indexed_fields
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
        search_params = {}

        # When we perform the fetch, we want to use stored fields
        for plugin in PluginImplementations(IDataSolr):
            search_params = plugin.datasolr_search(
                self.context, self.params, self.stored_fields, search_params
            )
        solr_query, solr_params = self.build_query(search_params, self.stored_fields)

        search = self.conn.query(solr_query, **solr_params)

        # If we have requested indexed only fields, then list of fields will be
        # those indexed; otherwise use the default stored fields
        fields = self.indexed_fields if self.indexed_only else self.stored_fields

        # Hide any internal fields - those starting with underscore (except for _id)
        fields = [f for f in fields if not f['id'].startswith('_') or f['id'] == '_id']

        # numFound isn't working with group fields, so auto-completes will
        # constantly be called - if there's a group field, set total to zero
        # if there's no records found - otherwise use numFound
        total = 0 if 'group_field' and not search.results else search.numFound
        response = dict(
            resource_id=self.resource_id,
            fields=fields,
            total=total,
            records=search.results,
        )
        requested_fields = [f['id'] for f in fields]
        # Date fields are returned as python datetime objects
        # So need to be converted into a string
        date_fields = [f['id'] for f in self.stored_fields if f['type'] == 'date' and f['id'] in requested_fields]
        if date_fields:
            for record in response['records']:
                for date_field in date_fields:
                    # TODO: This returns everything in one date (not time) format
                    # Identify the date depth and format accordingly
                    record[date_field] = record[date_field].strftime("%Y-%m-%d")

        if solr_params.get('facet_field'):
            response['facets'] = search.facet_counts

        return response

    @staticmethod
    def build_query(params, field_names):
        """ Build a solr query from API parameters

        @returns a dictionary defining SOLR request parameters
        """
        solr_query = []
        solr_params = dict(
            score=False,
            rows=params.get('limit', 100),
        )
        # Add fields to the params
        fields = params.get('fields', None)
        if fields:
            solr_params['fields'] = fields
        # Add offset
        offset = params.get('offset', None)
        if offset:
            solr_params['start'] = offset
        # Add distinct
        distinct = params.get('distinct', False)
        if distinct and fields:
            solr_params['group'] = 'true'
            solr_params['group_field'] = fields
            solr_params['group_main'] = 'true'

        # Add facets
        facets = params.get('facets', [])

        if facets:
            solr_params['facet'] = 'true'
            solr_params['facet_field'] = facets
            solr_params['facet_limit'] = params.get('facets_limit', 20),
            solr_params['facet_mincount'] = 1
            # Do we have individual facet field limits?
            facets_field_limit = params.get('facets_field_limit')
            if facets_field_limit:
                for facet_field, limit in facets_field_limit.items():
                    solr_param_key = 'f_%s_facet_limit' % facet_field
                    solr_params[solr_param_key] = limit

        # Ensure _id field is always selected first
        try:
            id_idx = solr_params['fields'].index('_id')
        except ValueError:
            solr_params['fields'].insert(0, '_id')
        else:
            # Move _id field to the start
            solr_params['fields'].insert(0, solr_params['fields'].pop(id_idx))

        # Q can be either a string, or a dictionary
        q = params.get('q', None)
        if isinstance(q, basestring):
            words = split_words(q, quotes=True)
            for word in words:
                solr_query.append('_fulltext:{}'.format(word))
        elif q:
            for field in q:
                if field not in field_names:
                    continue
                solr_query.append('{}:*{}*'.format(field, q['field']))

        filters = params.get('filters', None)
        if filters:
            filter_statements = params.get('filter_statements', {})
            for filter_field, filter_values in filters.items():
                # If we have a special filter statement for this query - add it
                #  e.g. _exclude_mineralogy =>  -collectionCode:MIN
                # Otherwise just add it as a generic filter - {}:"{}"
                try:
                    solr_query.append(filter_statements[filter_field])
                except KeyError:
                    filter_values = [filter_values] if not isinstance(filter_values, list) else filter_values
                    for filter_value in filter_values:
                        solr_query.append('{}:"{}"'.format(filter_field, filter_value))

        # If we have no solr query, then search for everything
        if not solr_query:
            solr_query.append('*:*')
        solr_query = ' AND '.join(solr_query)

        return solr_query, solr_params
