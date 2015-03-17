from nose.tools import assert_equals
from ckanext.datasolr.lib.solrqueryapi import SolrQueryApi, SolrQueryApiSql


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


class TestSolrQueryApi(object):
    def setUp(self):
        search_url='http://example.com/solr/select'
        solr_id_field='custom_id'
        self.solr_query_api = SolrQueryApi(
            search_url=search_url,
            solr_id_field=solr_id_field,
        )
        self.solr_query_api.solr = MockSolr(
            search_url, solr_id_field, 'AND',
            self.solr_query_api.solr.result_formatter
        )

    def test_field_translated_into_solr_query(self):
        """ Ensure that the field query provided is translated correctly """
        self.solr_query_api.fetch(
            resource_id='aaabbbccc',
            filters={
                'field1': 'value1',
                'field2': 'value2'
            }
        )
        sa = self.solr_query_api.solr.search_args['q']
        assert_equals(set(sa[0]), set(['field2:{}', 'field1:{}']))
        if sa[0][0] == 'field1:{}':
            assert_equals(sa[1], ['value1', 'value2'])
        else:
            assert_equals(sa[1], ['value2', 'value1'])

    def test_field_multiple_values_translated_into_disjoint_solr_query(self):
        """ Ensure that field query with multiple values is translated correctly """
        self.solr_query_api.fetch(
            resource_id='aaabbbccc',
            filters={
                'field1': ['value1', 'value2']
            }
        )
        sa = self.solr_query_api.solr.search_args['q']
        assert_equals(sa[0], ['(field1:{} OR field1:{})'])
        assert_equals(sa[1], ['value1', 'value2'])

    def test_fts_translated_into_solr_query(self):
        """ Ensure that full text query provided is translated correctly """
        self.solr_query_api.fetch(
            resource_id='aaabbbccc',
            q='the little brown fox'
        )
        sa = self.solr_query_api.solr.search_args['q']
        assert_equals(sa[0], ['_fulltext:{}']*4)
        assert_equals(set(sa[1]), set(['the', 'little', 'brown', 'fox']))

    def test_field_fts_translated_into_solr_query(self):
        """ Ensure that the field full text query provided is translated correctly """
        self.solr_query_api.fetch(
            resource_id='aaabbbccc',
            q={
                'field1': 'value1',
                'field2': 'value2'
            }
        )
        sa = self.solr_query_api.solr.search_args['q']
        assert_equals(set(sa[0]), set(['field2:*{}*', 'field1:*{}*']))
        if sa[0][0] == 'field1:*{}*':
            assert_equals(sa[1], ['value1', 'value2'])
        else:
            assert_equals(sa[1], ['value2', 'value1'])

    def test_filters_can_be_none(self):
        """ Ensure that q/filters can be none """
        self.solr_query_api.fetch(resource_id='aaabbbccc')

    def test_combined_filters(self):
        """ Ensure that providing field and full text query works """
        self.solr_query_api.fetch(
            resource_id='aaabbbccc',
            filters={'field1':'value1'},
            q='hello world'
        )
        sa = self.solr_query_api.solr.search_args['q']
        assert_equals(set(sa[0]), set(['field1:{}'] + ['_fulltext:{}']*2))
        assert_equals(set(sa[1]), set(['value1', 'hello', 'world']))

    def test_returned_data(self):
        """ Test the returned data is as expected """
        results = self.solr_query_api.fetch(resource_id='aabbcc', q='hello')
        assert_equals(results, (3, ['a', 'b', 'c']))


class TestSolrQueryApiSql(object):
    def setUp(self):
        search_url='http://example.com/solr/select'
        solr_id_field='custom_id'
        self.solr_query_api_sql = SolrQueryApiSql(
            search_url=search_url,
            solr_id_field=solr_id_field,
            id_field='other_id'
        )
        self.solr_query_api_sql.solr = MockSolr(
            search_url, solr_id_field, 'AND',
            self.solr_query_api_sql.solr.result_formatter
        )

    def test_query_sql_with_all_fields(self):
        """ Ensure the generated SQL fetches the rows as exepected """
        result = self.solr_query_api_sql.fetch(resource_id='aabbcc', q='hello')
        assert_equals(result[1].replace(' ', '').replace("\n", ''),
            'SELECT*FROM"aabbcc"WHEREother_id=ANY(VALUES(%s),(%s),(%s))'
        )

    def test_query_sql_with_selected_fields(self):
        """ Ensure the generated SQL fetches the rows as exepected """
        result = self.solr_query_api_sql.fetch(
            resource_id='aabbcc',
            q='hello',
            fields=['some_field', 'another_field']
        )
        assert_equals(result[1].replace(' ', '').replace("\n", ''),
            'SELECT"some_field","another_field"FROM"aabbcc"WHEREother_id=ANY(VALUES(%s),(%s),(%s))'
        )

    def test_query_sql_removes_field_double_quote(self):
        """ Ensure the generated SQL doesn't have double quotes in column names """
        result = self.solr_query_api_sql.fetch(
            resource_id='aabbcc',
            q='hello',
            fields=['some_"field', 'ano"t"her_field']
        )
        assert_equals(result[1].replace(' ', '').replace("\n", ''),
            'SELECT"some_field","another_field"FROM"aabbcc"WHEREother_id=ANY(VALUES(%s),(%s),(%s))'
        )

    def test_query_sql_values(self):
        """ Ensure the values for the generated SQL are correct """
        result = self.solr_query_api_sql.fetch(resource_id='aabbcc', q='hello')
        assert_equals(set(result[2]), set(['a', 'b', 'c']))
