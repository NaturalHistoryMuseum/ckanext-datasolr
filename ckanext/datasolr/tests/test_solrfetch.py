import json
from nose.tools import assert_true, assert_equals
from ckanext.datasolr.lib.solrfetch import SolrFetcher


class MockCnx(object):
    def __init__(self):
        self.sql = None
        self.values = None

    def execute(self, sql, values):
        self.sql = sql
        self.values = values
        return 'MockCnx::execute'


class MockSolr(object):
    def __init__(self, search_url, id_field='_id', query_type='AND',
                 result_formatter=None):
        # Store
        self.search_url = search_url
        self.id_field = id_field
        self.query_type = query_type
        self.result_formatter = result_formatter
        self.search_args = None
        # Return defaults
        self.result_row_count = 3
        self.result_rows = [
            {self.id_field: 'a'},
            {self.id_field: 'b'},
            {self.id_field: 'c'}
        ]

    def search(self, **kargs):
        self.search_args = kargs
        if self.result_formatter:
            rows = self.result_formatter(self.id_field, self.result_rows)
        else:
            rows = [r[self.id_field] for r in self.result_rows]
        return self.result_row_count, rows


class TestSolrFetch(object):
    def setUp(self):
        search_url='http://example.com/solr/select'
        solr_id_field='custom_id'
        self.cnx = MockCnx()
        self.solrfetcher = SolrFetcher(
            connection=self.cnx,
            search_url=search_url,
            solr_id_field=solr_id_field,
            id_field='other_id'
        )
        self.solrfetcher.solr = MockSolr(
            search_url, solr_id_field, 'AND',
            self.solrfetcher.solr.result_formatter
        )

    def test_field_translated_into_solr_query(self):
        """ Ensure that the field query provided is translated correctly """
        self.solrfetcher.fetch(
            resource_id='aaabbbccc',
            filters={
                'field1': 'value1',
                'field2': 'value2'
            }
        )
        sa = self.solrfetcher.solr.search_args['q']
        assert_equals(set(sa[0]), set(['field2:{}', 'field1:{}']))
        if sa[0][0] == 'field1:{}':
            assert_equals(sa[1], ['value1', 'value2'])
        else:
            assert_equals(sa[1], ['value2', 'value1'])

    def test_fts_translated_into_solr_query(self):
        """ Ensure that full text query provided is translated correctly """
        self.solrfetcher.fetch(
            resource_id='aaabbbccc',
            q='the little brown fox'
        )
        sa = self.solrfetcher.solr.search_args['q']
        assert_equals(sa[0], ['_fulltext:{}']*4)
        assert_equals(set(sa[1]), set(['the', 'little', 'brown', 'fox']))

    def test_field_fts_translated_into_solr_query(self):
        """ Ensure that the field full text query provided is translated correctly """
        self.solrfetcher.fetch(
            resource_id='aaabbbccc',
            q={
                'field1': 'value1',
                'field2': 'value2'
            }
        )
        sa = self.solrfetcher.solr.search_args['q']
        assert_equals(set(sa[0]), set(['field2:*{}*', 'field1:*{}*']))
        if sa[0][0] == 'field1:*{}*':
            assert_equals(sa[1], ['value1', 'value2'])
        else:
            assert_equals(sa[1], ['value2', 'value1'])

    def test_filters_can_be_none(self):
        """ Ensure that q/filters can be none """
        self.solrfetcher.fetch(resource_id='aaabbbccc')

    def test_combined_filters(self):
        """ Ensure that providing field and full text query works """
        self.solrfetcher.fetch(
            resource_id='aaabbbccc',
            filters={'field1':'value1'},
            q='hello world'
        )
        sa = self.solrfetcher.solr.search_args['q']
        assert_equals(set(sa[0]), set(['field1:{}'] + ['_fulltext:{}']*2))
        assert_equals(set(sa[1]), set(['value1', 'hello', 'world']))

    def test_query_is_executed(self):
        """ Ensure the query is executed """
        r = self.solrfetcher.fetch(resource_id='aabbcc', q='hello')
        assert_equals((3, 'MockCnx::execute'), r)

    def test_query_sql(self):
        """ Ensure the generated SQL fetches the rows as exepected """
        self.solrfetcher.fetch(resource_id='aabbcc', q='hello')
        assert_equals(
            self.cnx.sql.replace(' ', '').replace("\n", ''),
            'SELECT*FROM"aabbcc"WHEREother_id=ANY(VALUES(%s)(%s)(%s))'
        )

    def test_query_sql_values(self):
        """ Ensure the values for the generated SQL are correct """
        self.solrfetcher.fetch(resource_id='aabbcc', q='hello')
        assert_equals(set(self.cnx.values), set(['a', 'b', 'c']))
