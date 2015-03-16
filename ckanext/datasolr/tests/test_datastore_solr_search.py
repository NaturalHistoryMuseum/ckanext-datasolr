from ckanext.datasolr.lib.datastore_solr_search import DatastoreSolrSearch
from nose.tools import assert_equals, assert_in
from mock import Mock, patch
from threading import current_thread

_mock_solr = {}

class MockSolrQueryApiSql(object):
    def __init__(self, search_url, id_field, solr_id_field,
                 solr_resource_id_field=None):
        self.search_url = search_url
        self.id_field = id_field
        self.solr_id_field = solr_id_field
        self.solr_resource_id_field = solr_resource_id_field
        _mock_solr[current_thread().ident] = self

    def fetch(self, **query):
        self.query = query
        return 9000, 'sql-query',  (9,8,7)


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
            'solr_resource_id_field': None
        }
        self.solr_patcher = patch('ckanext.datasolr.lib.datastore_solr_search.SolrQueryApiSql', MockSolrQueryApiSql)
        self.solr_patcher.start()

    def teardown(self):
        self.solr_patcher.stop()
        del _mock_solr[current_thread().ident]

    def test_total_from_solr_is_returned(self):
        """ Ensure the total count returned by solr is returned """
        search = DatastoreSolrSearch({}, {'resource_id': 'some_resource'},
                                       self.config, self.connection)
        search._check_access = Mock(return_value=True)
        result = search.fetch()
        assert_equals(9000, result['total'])

    def test_sql_from_solrqueryapisql_is_executed(self):
        """ Ensure the SQL generated by solrqueyapisql is executed as expected """
        search = DatastoreSolrSearch({}, {'resource_id': 'some_resource'},
                                       self.config, self.connection)
        search._check_access = Mock(return_value=True)
        search.fetch()
        assert_equals('sql-query', self.connection.sql)
        assert_equals((9,8,7), self.connection.replacements)

    def test_converter_is_applied(self):
        """ Check that the converter is applied (decode json, etc.) """
        search = DatastoreSolrSearch({}, {'resource_id': 'some_resource'},
                                       self.config, self.connection)
        search._check_access = Mock(return_value=True)
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
        result = search.fetch()
        expected = [
            {'id': 'field1', 'type': 'type1'},
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
        result = search.fetch()
        assert_equals(result['q'], 'word')
        assert_equals(result['resource_id'], 'some_resource')
