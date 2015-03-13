import ckan.plugins as p
import ckanext.datastore.helpers as datastore_helpers
import ckanext.datastore.logic.schema as dsschema
import sqlalchemy

from ckan.lib.navl.dictization_functions import validate
from ckanext.datasolr.config import config
from ckanext.datasolr.lib.solrqueryapi import SolrQueryApiSql
from ckanext.datastore import db
from ckanext.datastore.logic.action import WHITELISTED_RESOURCES


class DatastoreSolrSearch(object):
    """ Class used to implement the datastore_solr_search action

    @param context: Ckan execution context
    @param params: Dictionary containing the action parameters
    @param connection: Database connection
    @raises ValidationError: If the request parameters are invalid
    @raises ObjectNotFound: If the resource is not found
    """
    def __init__(self, context, params, connection):
        self.context = context
        self.connection = connection
        schema = self.context.get('schema', dsschema.datastore_search_schema())
        self.params, errors = validate(params, schema, self.context)
        if errors:
            raise p.toolkit.ValidationError(errors)
        self.resource_id = self._resolve_resource_id(
            self.params['resource_id']
        )
        self.params['resource_id'] = self.resource_id
        self.fields = self._get_fields()
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

    def _get_fields(self):
        """ Return the list of fields & type for the given resource id

        TODO: can/should we cache this?

        @returns: Dict of field name to type
        """
        fields = {}
        all_fields = self.connection.execute(
            u'SELECT * FROM "{0}" LIMIT 1'.format(self.resource_id)
        )
        for field in all_fields.cursor.description:
            if field[0] == '_id' or not field[0].startswith('_'):
                name = field[0].decode('utf-8')
                field_type = db._get_type(
                    {'connection':self.connection}, field[1]
                )
                fields[name] = field_type
        return fields

    def _resolve_resource_id(self, resource_id):
        """ Checks the resource exists and resolve aliases

        @param resource_id: The resource id to check/resolve
        @raises ObjectNotFound: When the resource is not found
        """
        # Check the resource exists, resolve aliases and check access permission
        resources_sql = sqlalchemy.text(u'''SELECT alias_of FROM "_table_metadata"
                                            WHERE name = :id''')
        results = self.connection.execute(resources_sql, id=resource_id)
        if not results.rowcount > 0:
            raise p.toolkit.ObjectNotFound(p.toolkit._(
                'Resource "{0}" was not found.'.format(self.resource_id)
            ))

        if not resource_id in WHITELISTED_RESOURCES:
            orig_resource_id = results.fetchone()[0]
            if orig_resource_id:
                return orig_resource_id
        return resource_id

    def _check_access(self):
        """ Ensure we have access to the defined resource """
        p.toolkit.check_access('datastore_search', self.context, self.params)

    def fetch(self):
        """ Run the query and fetch the data
        """
        self._check_access()
        fetcher = SolrQueryApiSql(
            search_url=config[self.resource_id]['search_url'],
            id_field=config[self.resource_id]['id_field'],
            solr_id_field=config[self.resource_id]['solr_id_field']
        )
        fetch_params = ['resource_id', 'filters', 'q', 'limit', 'offset', 'sort', 'fields']
        params = {k: self.params[k] for k in fetch_params  if k in self.params}
        (total, sql, replacements) = fetcher.fetch(**params)
        sql = sql % tuple(replacements)
        query_result = self.connection.execute(sql, replacements)
        records = []
        for row in query_result:
            result_row = {}
            for field in self.params['fields']:
                result_row[field] = db.convert(row[field], self.fields[field])
            records.append(result_row)
        return {
            'fields': self.fields,
            'total': total,
            'records': records
        }
