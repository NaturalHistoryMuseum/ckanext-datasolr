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
        self.fields = self.conn.fields()

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
        data_dict = copy.deepcopy(self.params)
        for plugin in PluginImplementations(IDataSolr):
            data_dict = plugin.datasolr_validate(
                self.context, data_dict, self.fields
            )

        search_params = {}

        for plugin in PluginImplementations(IDataSolr):
            search_params = plugin.datasolr_search(
                self.context, self.params, self.fields, search_params
            )

        solr_query, solr_params = self.build_query(search_params)
        search = self.conn.query(solr_query, **solr_params)

        # If user has limited list of fields, then limit the schema definition
        if search_params.get('fields'):
            fields = [f for f in self.fields if f['id'] in search_params.get('fields')]
        else:
            fields = self.fields

        total = len(search.results) if 'group_field' in solr_params else search.numFound
        response = dict(
            resource_id=self.resource_id,
            fields=fields,
            total=total,
            records=search.results,
        )

        if solr_params.get('facet_field'):
            response['facets'] = search.facet_counts

        return response

    def build_query(self, params):
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
        facets = params.get('facets', False)

        print(facets)

        if facets:
            solr_params['facet'] = 'true'
            solr_params['facet_field'] = facets
            solr_params['facet_limit'] = 20

        # Ensure _id field is always selected first
        try:
            id_idx = solr_params['fields'].index('_id')
        except ValueError:
            solr_params['fields'].insert(0, '_id')
        else:
            # Move _id field to the start
            solr_params['fields'].insert(0, solr_params['fields'].pop(id_idx))

        q = params.get('q', None)
        if q:
            words = split_words(q, quotes=True)
            for word in words:
                solr_query.append('_fulltext:{}'.format(word))
        filters = params.get('filters', None)
        if filters:
            for filter_field, filter_values in filters.items():
                for filter_value in filter_values:
                    solr_query.append('{}:"{}"'.format(filter_field, filter_value))

        # If we have no solr query, then search for everything
        if not solr_query:
            solr_query.append('*:*')
        solr_query = ' AND '.join(solr_query)

        return solr_query, solr_params
