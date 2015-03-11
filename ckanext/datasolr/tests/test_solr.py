import json
import httpretty

from nose.tools import assert_equals
from ckanext.datasolr.lib.solr import Solr

class TestSolr():

    def setUp(self):
        self.solr = Solr(
            search_url='http://custom_domain/path/custom_rq',
            id_field='custom_id',
            query_type='CUSTOM_OP'
        )
        httpretty.enable()
        httpretty.register_uri(
            httpretty.GET,
            'http://custom_domain/path/custom_rq',
            body=json.dumps({
                'response': {
                    'numFound': 3,
                    'docs': [
                        {'custom_id': '-a-'},
                        {'custom_id': '~b~'},
                        {'custom_id': '*c*'}
                    ]
                }
            })
        )

    def tearDown(self):
        httpretty.disable()

    def test_search_queries_solr_server(self):
        """ Ensure running a search queries the SOLR server at the expected path """
        self.solr.search(q='*:*')
        lr_url = httpretty.last_request().path.split('?')[0]
        assert_equals(lr_url, '/path/custom_rq')

    def test_search_queries_solr_with_correct_arguments(self):
        """ Ensure running a search queries SOLR with the expected arguments in the query string """
        self.solr.search(q='*:*')
        qs = httpretty.last_request().querystring
        assert_equals(qs['wt'], ['json'])
        assert_equals(qs['fl'], ['custom_id'])
        assert_equals(qs['q'], ['*:*'])

    def test_q_argument_placeholders(self):
        """ Test placeholders are inserted in the q argument """
        self.solr.search(q=('field1:{} AND field2:{}', ['value1', 'value2']))
        qs = httpretty.last_request().querystring
        assert_equals(qs['q'], ['field1:value1 AND field2:value2'])

    def test_q_argument_placeholders_are_escaped(self):
        """ Test placeholders inserted in q are escaped """
        self.solr.search(q=('field1:{} AND field2:{}', ['v(alue) 1!', '[val :-) ue2]']))
        qs = httpretty.last_request().querystring
        assert_equals(qs['q'], ['field1:v\(alue\)\ 1\! AND field2:\[val\ \:\-\)\ ue2\]'])

    def test_multiclose_q_build_with_default_query_type(self):
        """ Test a multi-clause q argument is build with the defined query type """
        self.solr.search(q=(['a', 'b', 'c'], []))
        qs = httpretty.last_request().querystring
        assert_equals(qs['q'], ['a CUSTOM_OP b CUSTOM_OP c'])

    def test_multiclose_q_gets_correct_replacement(self):
        """ Test a multi-clause q argument is build with the correct replacement """
        self.solr.search(q=(['a:{}', 'b:{}', 'c:{}'], ['v1', 'v2', 'v3']))
        qs = httpretty.last_request().querystring
        assert_equals(qs['q'], ['a:v1 CUSTOM_OP b:v2 CUSTOM_OP c:v3'])

    def test_escape_solr_query_string(self):
        """ Test the solr query string escape """
        chars = '+-&|!(){}[]^~*?:"; /'
        assert_equals(
            ''.join(['\\' + c for c in chars]),
            self.solr.escape(chars)
        )

    def test_returned_row_count(self):
        """ Ensure the returned row count is the one provided by SOLR """
        r = self.solr.search(q='*:*')
        assert_equals(3, r[0])

    def test_returned_rows(self):
        """ Ensure the returned rows are the ones returned by solr, using the
            default formatting. """
        r = self.solr.search(q='*:*')
        assert_equals(['-a-', '~b~', '*c*'], r[1])


    def test_custom_result_formatter(self):
        """ Test we can use a custom result formatter """
        def format_to_string(id, rows):
            return ','.join(r[id] for r in rows)
        solr = Solr(
            search_url='http://custom_domain/path/custom_rq',
            id_field='custom_id',
            query_type='CUSTOM_OP',
            result_formatter=format_to_string
        )
        r = solr.search(q='*:*')
        assert_equals('-a-,~b~,*c*', r[1])
