import re
from nose.tools import assert_equals, assert_in, assert_not_in
from ckanext.datasolr.lib.solrqueryapi import ApiQueryToSolr, SolrQueryToSql


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


class TestApiQueryToSolr(object):
    def setup(self):
        self.api_to_solr = ApiQueryToSolr(
            solr_id_field='custom_id',
            field_types={'field1': 'type1', 'field2': 'type2'}
        )

    def test_validate_strips_out_known_fields_from_filters(self):
        """ Check that validate strips known fields out of filters"""
        q, filters = self.api_to_solr.validate(
            q=None,
            filters={'field1': 'value1', 'field3': 'value3'}
        )
        assert_equals(filters, {'field3': 'value3'})

    def test_validate_strips_out_known_fields_from_q(self):
        """ Check that validate strips known fields out of filters"""
        q, filters = self.api_to_solr.validate(
            filters=None,
            q={'field1': 'value1', 'field3': 'value3'}
        )
        assert_equals(q, {'field3': 'value3'})

    def test_validate_sets_search_string_to_none(self):
        """ Check that validate sets q to none when it is a search string """
        q, filters = self.api_to_solr.validate(
            q='search string',
            filters=None
        )
        assert_equals(q, None)

    def test_validate_accepts_empty_input(self):
        """ Check that works with None as values """
        q, filters = self.api_to_solr.validate(
            q=None,
            filters=None
        )
        assert_equals(q, None)
        assert_equals(filters, None)

    def test_build_query_sets_rows_and_start(self):
        """ Check build_query sets rows and start from offset and limit """
        solr_args = self.api_to_solr.build_query(
            resource_id='aaa',
            offset=12,
            limit=35
        )
        assert_equals(solr_args['start'], 12)
        assert_equals(solr_args['rows'], 35)

    def test_build_query_sets_groups_from_distinct(self):
        """ Check build_query uses distinct to create a group """
        solr_args = self.api_to_solr.build_query(
            resource_id='aaa',
            distinct='field1'
        )
        assert_equals(solr_args['group'], 'true')
        assert_equals(solr_args['group.field'], 'field1')

    def test_build_query_sets_sort(self):
        """ Check build query applies provided term sort """
        # Single term
        solr_args = self.api_to_solr.build_query(
            resource_id='aaa',
            sort='field1 DESC, field2 ASC'
        )
        assert_equals(solr_args['sort'], 'field1 DESC, field2 ASC')

    def test_build_query_sets_sort_multiple_default(self):
        """ Check build query provides a default sort """
        solr_args = self.api_to_solr.build_query(
            resource_id='aaa',
            sort='field1, field2'
        )
        assert_equals(solr_args['sort'], 'field1 ASC, field2 ASC')

    def test_build_query_unquotes_sort_fields(self):
        """ Check build query unquotes sort terms """
        solr_args = self.api_to_solr.build_query(
            resource_id='aaa',
            sort='"field1" ASC, "field2" DESC'
        )
        assert_equals(solr_args['sort'], 'field1 ASC, field2 DESC')

    def test_build_query_filters(self):
        """ Test the build query builds from the filters """
        solr_args = self.api_to_solr.build_query(
            resource_id='aaa',
            filters={'field1': 'search1', 'field2': 'search2'}
        )
        assert_equals(set(solr_args['q'][0]), set(['field1:{}', 'field2:{}']))
        if solr_args['q'][1][0] == 'field1:{}':
            assert_equals(solr_args['q'][1], ['search1', 'search2'])
        else:
            assert_equals(solr_args['q'][1], ['search2', 'search1'])

    def test_build_query_fts(self):
        """ Test the build query builds from the full text search"""
        solr_args = self.api_to_solr.build_query(
            resource_id='aaa',
            q='a carrot cake'
        )
        assert_equals(solr_args['q'][0], ['_fulltext:{}']*3)
        assert_equals(set(solr_args['q'][1]), set(['a', 'carrot', 'cake']))

    def test_build_query_field_fts(self):
        """ Test the build query builds from the field fts """
        solr_args = self.api_to_solr.build_query(
            resource_id='aaa',
            q={'field1': 'search1', 'field2': 'search2'}
        )
        assert_equals(set(solr_args['q'][0]), set(['field1:*{}*', 'field2:*{}*']))
        if solr_args['q'][1][0] == 'field1:*{}*':
            assert_equals(solr_args['q'][1], ['search1', 'search2'])
        else:
            assert_equals(solr_args['q'][1], ['search2', 'search1'])

    def test_build_query_strips_field_fts_postgres_syntax(self):
        """ Test the build query strips postgres suffic match syntax """
        solr_args = self.api_to_solr.build_query(
            resource_id='aaa',
            q={'field1': 'search1:*', 'field2': 'search2:*'}
        )
        assert_equals(set(solr_args['q'][1]), set(['search1', 'search2']))


class TestSolrQueryResultToSql(object):
    def setup(self):
        self.solr_to_sql = SolrQueryToSql(
            search_url='http://example.com/solr/select',
            id_field='id_field',
            solr_id_field='solr_id_field'
        )
        self.solr_to_sql.solr = MockSolr(
            search_url='http://example.com/solr/select',
            id_field='solr_id_field',
            query_type='AND',
            result_formatter=self.solr_to_sql.solr.result_formatter
        )

    def test_solr_row_count_returned(self):
        """ Ensure the row count provided by SOLR is returned """
        (total, sql, values) = self.solr_to_sql.fetch('aaa', solr_args={'q':'*:*'})
        assert_equals(total, 3)

    def test_sql_contains_id_match(self):
        """ Ensure that the returned SQL matches the ID against the defined values """
        (total, sql, values) = self.solr_to_sql.fetch('aaa', solr_args={'q':'*:*'})
        sql = re.sub('[ \n\r]', '', sql)
        assert_in('"id_field"=ANY(VALUES(%s),(%s),(%s))', sql)

    def test_replacement_values(self):
        """ Test the replacement values are correct"""
        (total, sql, values) = self.solr_to_sql.fetch('aaa', solr_args={'q':'*:*'})
        assert_equals(['a', 'b', 'c'], values)

    def test_select_all_fields_by_default(self):
        """ Test all fields are selected by default """
        (total, sql, values) = self.solr_to_sql.fetch('aaa', solr_args={'q':'*:*'})
        sql = re.sub('[ \n\r]', '', sql)
        assert_in('SELECT*FROM', sql)

    def test_select_given_fields(self):
        """ Test given fields are selected """
        (total, sql, values) = self.solr_to_sql.fetch(
            'aaa', solr_args={'q':'*:*'}, fields=['field1', 'field2']
        )
        sql = re.sub('[ \n\r]', '', sql)
        assert_in('SELECT"field1","field2"FROM', sql)

    def test_no_default_order(self):
        """ Test there is no order clause by default """
        (total, sql, values) = self.solr_to_sql.fetch('aaa', solr_args={'q':'*:*'})
        sql = re.sub('[ \n\r]', '', sql)
        assert_not_in('order', sql.lower())

    def test_given_order(self):
        """ Test the given order is parsed and applied to the sql """
        (total, sql, values) = self.solr_to_sql.fetch(
            'aaa',  solr_args={'q':'*:*'}, sort='field1, field2 DESC'
        )
        sql = re.sub('[ \n\r]', '', sql)
        assert_in('ORDERBY"field1"ASC,"field2"DESC', sql)
