import ckan.plugins as p
import ckanext.datastore.helpers as datastore_helpers
import ckanext.datastore.logic.schema as dsschema

from ckan.lib.navl.dictization_functions import validate
from ckanext.datasolr.lib.solrqueryapi import SolrQueryApiSql


class DatastoreSolrSearch(object):
    """ Class used to implement the datastore_solr_search action

    @param context: Ckan execution context
    @param params: Dictionary containing the action parameters
    @param config: Datasolr configuration for this resource
    @param connection: Database connection object
        (ckanext.datasolr.lib.db.Connection)
    @raises ValidationError: If the request parameters are invalid
    @raises ObjectNotFound: If the resource is not found
    """
    def __init__(self, context, params, config, connection):
        self.context = context
        self.config = config
        self.connection = connection
        schema = self.context.get('schema', dsschema.datastore_search_schema())
        self.params, errors = validate(params, schema, self.context)
        if errors:
            raise p.toolkit.ValidationError(errors)
        try:
            self.resource_id = self.connection.resolve_alias(
                self.params['resource_id']
            )
        except ValueError:
            raise p.toolkit.ObjectNotFound(p.toolkit._(
                'Resource "{0}" was not found.'.format(self.params['resource_id'])
            ))
        self.params['resource_id'] = self.resource_id
        self.fields = self.connection.get_fields(self.resource_id)
        if 'fields' in self.params:
            fields = datastore_helpers.get_list(self.params['fields'])
            fields = list(set(fields) & set(self.fields.keys()))
            if len(fields) == 0:
                raise p.toolkit.ValidationError({
                    'query': 'Invalid field list'
                })
            else:
                self.params['fields'] = fields
        else:
            self.params['fields'] = self.fields.keys()

    def _check_access(self):
        """ Ensure we have access to the defined resource """
        p.toolkit.check_access('datastore_search', self.context, self.params)

    def fetch(self):
        """ Run the query and fetch the data
        """
        self._check_access()
        fetcher = SolrQueryApiSql(
            search_url=self.config['search_url'],
            id_field=self.config['id_field'],
            solr_id_field=self.config['solr_id_field'],
            solr_resource_id_field=self.config['solr_resource_id_field']
        )
        fetch_params = ['resource_id', 'filters', 'q', 'limit', 'offset', 'sort', 'fields']
        params = {k: self.params[k] for k in fetch_params  if k in self.params}
        (total, sql, replacements) = fetcher.fetch(**params)
        records = self.connection.execute(
            sql, replacements, row_formatter=self._format_row
        )
        # TODO: should we cache this?
        api_field_list = [{'id': f, 'type': self.fields[f]} for f in self.fields]
        return dict(params.items() + {
            'fields': api_field_list,
            'total': total,
            'records': records,
            '_backend': 'datasolr'
         }.items())

    def _format_row(self, row):
        """ Format a row of results

        @param row: A dict like object containing the fields
        """
        result = {}
        for field in self.params['fields']:
            result[field] = self.connection.convert(
                row[field], self.fields[field]
            )
        return result
