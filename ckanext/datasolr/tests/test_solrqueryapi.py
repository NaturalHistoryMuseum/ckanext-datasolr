import re
from nose.tools import assert_equals, assert_raises, assert_not_in
from ckanext.datasolr.lib.solrqueryapi import SolrQueryApi, SolrQueryApiSql


def custom_field_mapper(field_name):
    """ A custom field mapper

    This removes non alphanumeric characters, and
    lower cases the string
    """
    return re.sub('[^a-z0-9]', '', field_name.lower())


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

    def test_field_query_into_solr(self):
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

    def test_default_field_mapper_is_applied(self):
        """ Ensure the default field mapper is applied """
        self.solr_query_api.fetch(
            resource_id='aaabbbccc',
            filters={'_F,i;e"l\'-d]1': 'value1'},
            q={'f""ie-();lD_2': 'value2'},
            sort='f$IEL*d-3 ASC'
        )
        sa = self.solr_query_api.solr.search_args
        assert_equals(set(sa['q'][0]), set(['_Field1:{}', 'fielD_2:*{}*']))
        assert_equals(sa['sort'], 'fIELd3 ASC')

    def test_custom_field_mapper_is_applied(self):
        search_url='http://example.com/solr/select'
        solr_id_field='custom_id'
        solr_query_api = SolrQueryApi(
            search_url=search_url,
            solr_id_field=solr_id_field,
            field_mapper=custom_field_mapper
        )
        solr_query_api.solr = MockSolr(
            search_url, solr_id_field, 'AND',
            self.solr_query_api.solr.result_formatter
        )
        solr_query_api.fetch(
            resource_id='aaabbbccc',
            filters={'_F,i;e"l\'-d]1': 'value1'},
            q={'f""ie-();lD_2': 'value2'},
            sort='f$IEL*d-3 ASC'
        )
        sa = solr_query_api.solr.search_args
        assert_equals(set(sa['q'][0]), set(['field1:{}', 'field2:*{}*']))
        assert_equals(sa['sort'], 'field3 ASC')

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

    def test_distinct_field_is_sent_as_group(self):
        """ Ensure that distinct field is sent to solr as group query """
        results = self.solr_query_api.fetch(resource_id='aabbcc',
                                            distinct='distinctfield')
        sa = self.solr_query_api.solr.search_args
        assert_equals(sa['group'], 'true')
        assert_equals(sa['group.main'], 'true')
        assert_equals(sa['group.field'], 'distinctfield')

    def test_no_group_when_not_distinct(self):
        """ Ensure that group query is not used when distinct isnt' specifed"""
        results = self.solr_query_api.fetch(resource_id='aabbcc')
        sa = self.solr_query_api.solr.search_args
        assert_not_in('group', sa)

    def test_sort_is_applied(self):
        """ Ensure the sort is sent to SOLR """
        results = self.solr_query_api.fetch(resource_id='aabbcc',
                                            sort='field1 DESC')
        sa = self.solr_query_api.solr.search_args
        assert_equals(sa['sort'], 'field1 DESC')

    def test_sort_is_set_to_asc_by_default(self):
        """ Ensure the sort is set to ASC by default """
        results = self.solr_query_api.fetch(resource_id='aabbcc',
                                            sort='field1')
        sa = self.solr_query_api.solr.search_args
        assert_equals(sa['sort'], 'field1 ASC')

    def test_sort_parses_complex_sorts(self):
        """ Test a complex sort statement (with default field mapper) """
        results = self.solr_query_api.fetch(resource_id='aabbcc',
                                            sort='"field 1" ASC, "field, 2", ",field "" 3,", "field 4 DESC"')
        sa = self.solr_query_api.solr.search_args
        assert_equals(sa['sort'], 'field1 ASC, field2 ASC, field3 ASC, field4DESC ASC')

       
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

    def test_distinct_applies_distinct_field(self):
        """ Ensure that a distinct query applies the group fields """
        result = self.solr_query_api_sql.fetch(resource_id='aabbcc',
                                               fields=['field1'],
                                               distinct=True)
        sa = self.solr_query_api_sql.solr.search_args
        assert_equals(sa['group'], 'true')
        assert_equals(sa['group.main'], 'true')
        assert_equals(sa['group.field'], 'field1')

    def test_multiple_distinct_fields_raise(self):
        """ Test that attempting to define multiple distinct fields raises """
        assert_raises(ValueError, self.solr_query_api_sql.fetch,
                      resource_id='a', fields=['f1', 'f2'],
                      distinct=True)

    def test_distinct_with_no_defined_field_does_nothing(self):
        """ Ensure that selecting 'distinct' with no fields does nothing """
        result = self.solr_query_api_sql.fetch(resource_id='aabbcc',
                                               distinct=True)
        sa = self.solr_query_api_sql.solr.search_args
        assert_not_in('group', sa)
