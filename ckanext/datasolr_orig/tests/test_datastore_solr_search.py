import ckan.plugins as p
import copy

from ckanext.datasolr.lib.datastore_solr_search import DatastoreSolrSearch
from ckanext.datasolr.plugin import DataSolrPlugin
from nose.tools import assert_equals, assert_in, assert_raises
from mock import Mock, patch
from threading import current_thread

_mock_solr = {}
_mock_plugin = {}


class MockPlugin(object):
    def __init__(self):
        _mock_plugin[current_thread().ident] = self
        self.validate = {}
        self.search = {}

    def datasolr_validate(self, context, data_dict, fields):
        """ Remove data_dict['_test'] and data_dict['filters']['_test']
            to validate them """
        self.validate = {
            'context': copy.deepcopy(context),
            'data_dict': copy.deepcopy(data_dict),
            'fields': copy.deepcopy(fields)
        }
        if 'filters' in data_dict and '_test' in data_dict['filters']:
            del data_dict['filters']['_test']
        if 'fields' in data_dict and '_test' in data_dict['fields']:
            data_dict['fields'].remove('_test')
        if 'sort' in data_dict and ('_test', 'ASC') in data_dict['sort']:
            data_dict['sort'].remove(('_test', 'ASC'))
        return data_dict

    def datasolr_search(self, context, data_dict, fields, query_dict):
        self.search = {
            'context': context,
            'data_dict': data_dict,
            'fields': fields,
            'query_dict': query_dict
        }
        if 'filters' in data_dict and '_test' in data_dict['filters']:
            query_dict['q'][0].append('field1:{}')
            query_dict['q'][1].append('test value')
            query_dict['new'] = 'new'
            query_dict['fields'].append('field2')
        return query_dict


def plugin_implementations(cls):
    return [DataSolrPlugin(), MockPlugin()]


class MockSolrQueryToSql(object):
    def __init__(self, search_url, id_field, solr_id_field,
                 solr_resource_id_field=None):
        self.search_url = search_url
        self.id_field = id_field
        self.solr_id_field = solr_id_field
        self.solr_resource_id_field = solr_resource_id_field
        _mock_solr[current_thread().ident] = self

    def fetch(self, **query):
        self.query = query
        return {
            'total': 9000,
            'sql': 'sql-query',
            'values': (9,8,7),
            'stats': {
                'stats_fields': {
                    'field1': {
                        'sum': 10
                    }
                }
            }
        }


class MockConnection(object):
    def __init__(self, fields, alias, result):
        self.fields = fields
        self.alias = alias
        self.result = result
        self.fields_table = None
        self.resolve_table = None
        self.sql = None
        self.replacements = None

    def get_fields(self, table):
        self.fields_table = table
        return self.fields

    def resolve_alias(self, table):
        self.resolve_table = table
        return self.alias

    def execute(self, sql, replacements, row_formatter=None):
        self.sql = sql
        self.replacements = replacements
        if row_formatter:
            return [row_formatter(r) for r in self.result]
        else:
            return self.result

    def convert(self, value, field_type):
        return '{}-{}'.format(value, field_type)


class TestDatastoreSolrSearch(object):
    def setup(self):
        self.connection = MockConnection(
            {'field1': 'type1', 'field2': 'type2'},
            'some_resource',
            [
                {'field1': 'row1-field1', 'field2': 'row1-field2'},
                {'field1': 'row2-field1', 'field2': 'row2-field2'},
                {'field1': 'row3-field1', 'field2': 'row3-field2'},
            ]
        )
        self.config = {
            'search_url': 'http://localhost/solr/select',
            'id_field': 'postgres_id_field',
            'solr_id_field': 'solr_id_field',
            'solr_resource_id_field': None,
            'field_mapper': 'ckanext.datasolr.lib.solrqueryapi.default_field_mapper'
        }
        self.solr_patcher = patch('ckanext.datasolr.lib.datastore_solr_search.SolrQueryToSql', MockSolrQueryToSql)
        self.plugin_patcher = patch('ckanext.datasolr.lib.datastore_solr_search.PluginImplementations', plugin_implementations)
        self.solr_patcher.start()
        self.plugin_patcher.start()

    def teardown(self):
        self.solr_patcher.stop()
        self.plugin_patcher.stop()
        thread_ident = current_thread().ident
        if thread_ident in _mock_solr:
            del _mock_solr[thread_ident]
        if thread_ident in _mock_plugin:
            del _mock_plugin[thread_ident]

    def test_total_from_solr_is_returned(self):
        """ Ensure the total count returned by solr is returned """
        search = DatastoreSolrSearch({}, {'resource_id': 'some_resource'},
                                       self.config, self.connection)
        search._check_access = Mock(return_value=True)
        search.validate()
        result = search.fetch()

        assert_equals(9000, result['total'])

    def test_sql_from_solrqueryapisql_is_executed(self):
        """ Ensure the SQL generated by solrqueyapisql is executed as expected """
        search = DatastoreSolrSearch({}, {'resource_id': 'some_resource'},
                                       self.config, self.connection)
        search._check_access = Mock(return_value=True)
        search.validate()
        search.fetch()
        assert_equals('sql-query', self.connection.sql)
        assert_equals((9,8,7), self.connection.replacements)

    def test_converter_is_applied(self):
        """ Check that the converter is applied (decode json, etc.) """
        search = DatastoreSolrSearch({}, {'resource_id': 'some_resource'},
                                       self.config, self.connection)
        search._check_access = Mock(return_value=True)
        search.validate()
        result = search.fetch()
        assert_equals([
            {'field1': 'row1-field1-type1', 'field2': 'row1-field2-type2'},
            {'field1': 'row2-field1-type1', 'field2': 'row2-field2-type2'},
            {'field1': 'row3-field1-type1', 'field2': 'row3-field2-type2'},
        ], result['records'])

    def test_field_definition_is_returned(self):
        """ Check the field definition is returned """
        search = DatastoreSolrSearch({}, {'resource_id': 'some_resource'},
                                       self.config, self.connection)
        search._check_access = Mock(return_value=True)
        search.validate()
        result = search.fetch()
        expected = [
            {'id': 'field1', 'type': 'type1', 'sum': 10},
            {'id': 'field2', 'type': 'type2'}
        ]
        assert_equals(len(expected), len(result['fields']))
        for fd in result['fields']:
            assert_in(fd, expected)

    def test_table_alias_is_applied(self):
        """ Ensure that the table alias is applied """
        search = DatastoreSolrSearch({}, {'resource_id': 'aaa'},
                                       self.config, self.connection)
        search._check_access = Mock(return_value=True)
        search.validate()
        search.fetch()
        solr = _mock_solr[current_thread().ident]
        assert_equals('some_resource', solr.query['resource_id'])

    def test_params_are_repeated_in_response(self):
        search = DatastoreSolrSearch(
            {},
            {'resource_id': 'some_resource', 'q': 'word'},
           self.config, self.connection
        )
        search._check_access = Mock(return_value=True)
        search.validate()
        result = search.fetch()
        assert_equals(result['q'], 'word')
        assert_equals(result['resource_id'], 'some_resource')

    def test_validation_fails_with_unknown_field(self):
        """ Test validation fails with unknown fields """
        search = DatastoreSolrSearch(
            {},
            {'resource_id': 'some_resource', 'fields': 'field1, other'},
           self.config, self.connection
        )
        search._check_access = Mock(return_value=True)
        assert_raises(p.toolkit.ValidationError, search.validate)

    def test_validation_fails_with_unknown_sort(self):
        """ Test validation fails with unknown sorts """
        search = DatastoreSolrSearch(
            {},
            {'resource_id': 'some_resource', 'sort': 'field1, other'},
           self.config, self.connection
        )
        search._check_access = Mock(return_value=True)
        assert_raises(p.toolkit.ValidationError, search.validate)

    def test_validation_fails_with_unknown_filters(self):
        """ Test validation fails with unknown fields """
        search = DatastoreSolrSearch(
            {},
            {'resource_id': 'some_resource', 'filters': {'v':'v'}},
           self.config, self.connection
        )
        search._check_access = Mock(return_value=True)
        assert_raises(p.toolkit.ValidationError, search.validate)

    def test_validation_from_plugin(self):
        """ Test that fields/sorts/filters validated by plugins
            do not cause a validation error
        """
        data_dict = {
            'resource_id': 'some_resource',
            'fields': 'field1, _test',
            'sort': 'field1, _test',
            'filters': {'field1': 'v1', '_test': 'v2'}
        }
        search = DatastoreSolrSearch(
            {}, data_dict,
           self.config, self.connection
        )
        search._check_access = Mock(return_value=True)
        try:
            search.validate()
        except p.toolkit.ValidationError:
            assert False

    def test_plugin_validate_is_invoked_with_data_dict(self):
        """ Test that plugins validate methods are invoked with the data dict and
            only unvalidated fields are left.
        """
        data_dict = {
            'resource_id': 'some_resource',
            'fields': 'field1, _test',
            'sort': 'field1, _test',
            'filters': {'field1': 'v1', '_test': 'v2'}
        }
        search = DatastoreSolrSearch(
            {}, data_dict,
           self.config, self.connection
        )
        search._check_access = Mock(return_value=True)
        search.validate()
        p = _mock_plugin[current_thread().ident]
        assert_equals(p.validate['data_dict']['resource_id'], 'some_resource')
        assert_equals(p.validate['data_dict']['fields'], ['_test'])
        assert_equals(p.validate['data_dict']['sort'], [('_test', 'ASC')])
        assert_equals(p.validate['data_dict']['filters'], {'_test':'v2'})

    def test_plugin_validate_context_includes_api_to_solr(self):
        """ Ensure the api_to_solr object is available to plugins """
        search = DatastoreSolrSearch(
            {}, {'resource_id': 'some_resource'},
           self.config, self.connection
        )
        search._check_access = Mock(return_value=True)
        search.validate()
        p = _mock_plugin[current_thread().ident]
        assert_in('api_to_solr', p.validate['context'])

    def test_plugin_can_edit_solr_query(self):
        search = DatastoreSolrSearch(
            {}, {'resource_id': 'aaa', 'filters':{'_test':'v'}},
            self.config, self.connection)
        search._check_access = Mock(return_value=True)
        search.validate()
        search.fetch()
        solr = _mock_solr[current_thread().ident]
        assert_equals((['*:*', 'field1:{}'], ['test value']), solr.query['solr_args']['q'])
        assert_equals('new', solr.query['solr_args']['new'])

    def test_plugin_can_edit_fields_to_fetch(self):
        search = DatastoreSolrSearch(
            {},
            {
                'resource_id': 'aaa',
                'filters':{'_test':'v'},
                'fields': 'field1'
            },
            self.config, self.connection)
        search._check_access = Mock(return_value=True)
        search.validate()
        search.fetch()
        solr = _mock_solr[current_thread().ident]
        assert_equals(solr.query['fields'], ['field1', 'field2'])

    def test_solr_field_stats_query_processed(self):
        """ Ensure the field stats queries are passed on to solr """
        search = DatastoreSolrSearch(
             {},
             {
                 'resource_id': 'aaa',
                 'solr_stats_fields': 'field1, field2'
             },
             self.config, self.connection)
        search._check_access = Mock(return_value=True)
        search.validate()
        search.fetch()
        solr = _mock_solr[current_thread().ident]
        assert_equals(solr.query['solr_args']['stats'], 'true')
        assert_equals(solr.query['solr_args']['stats.field'], ['field1', 'field2'])

    def test_solr_not_empty_search_applied(self):
        """ Ensure the not empty search is applied """
        search = DatastoreSolrSearch(
             {},
             {
                 'resource_id': 'aaa',
                 'filters': {
                     '_solr_not_empty': ['field1', 'field2']
                 }
             },
             self.config, self.connection)
        search._check_access = Mock(return_value=True)
        search.validate()
        search.fetch()
        solr = _mock_solr[current_thread().ident]
        q = solr.query['solr_args']['q']
        assert_in('field1:[* TO *]', q[0])
        assert_in('field2:[* TO *]', q[0])
