from ckanext.datasolr.lib.helpers import parse_sort_statement
from nose.tools import assert_equals


class TestHelpers(object):
    def test_parse_sort_single_field(self):
        """ Test parse sort works on a single field with no defined order """
        s = parse_sort_statement('field1')
        assert_equals([('field1', 'ASC')], s)

    def test_parse_sort_single_field_with_order(self):
        """ Test parse sort works on a single field with a defined order """
        s = parse_sort_statement('field1 DESC')
        assert_equals([('field1', 'DESC')], s)

    def test_parse_sort_multiple_field(self):
        """ Test parse sort works on multiple fields with no defined order """
        s = parse_sort_statement('field1, field2, field3')
        assert_equals([('field1', 'ASC'), ('field2', 'ASC'), ('field3', 'ASC')], s)

    def test_parse_sort_multiple_field_with_order(self):
        """ Test parse sort works on multiple fields with a defined order """
        s = parse_sort_statement('field1 DESC, field2 ASC, field3 DESC')
        assert_equals([('field1', 'DESC'), ('field2', 'ASC'), ('field3', 'DESC')], s)

    def test_parse_sort_unquotes_fields(self):
        """ Ensure parse_sort_statement unquotes fields """
        s = parse_sort_statement('"field1", "field2", "field3"')
        assert_equals([('field1', 'ASC'), ('field2', 'ASC'), ('field3', 'ASC')], s)

    def test_parse_sort_does_not_split_on_comma_between_quotes(self):
        """ Ensure parse_sort_statement does not split on commas between quotes """
        s = parse_sort_statement('"field1, it is", "field2"')
        assert_equals([('field1, it is', 'ASC'), ('field2', 'ASC')], s)

    def test_parse_sort_does_not_read_order_between_quotes(self):
        """ Ensure parse_sort_statement doesn't read DESC when between quotes """
        s = parse_sort_statement('"field1 DESC"')
        assert_equals([('field1 DESC', 'ASC')], s)
