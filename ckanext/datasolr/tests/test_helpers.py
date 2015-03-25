from ckanext.datasolr.lib.helpers import parse_sort_statement, split_words
from nose.tools import assert_equals


class TestSplitWords(object):
    def test_split_no_quotes_splits_on_all_spaces(self):
        """ Ensure that splitting with quotes=False splits on all spaces """
        assert_equals(['the', '"little', 'brown"', 'fox'],
            split_words('the "little brown" fox', quotes=False)
        )

    def test_split_no_quotes_strips_extra_spaces(self):
        """ Ensure that splitting with quotes=False removes extra spaces """
        assert_equals(['the', 'little', 'brown', 'fox'],
            split_words('  the   little   brown   fox  ', quotes=False)
       )

    def test_split_on_spaces(self):
        """ Ensure that splitting works on spaces """
        assert_equals(['the', 'little', 'brown', 'fox'],
            split_words('the little brown fox')
        )

    def test_split_keeps_quoted_content_and_removes_quotes(self):
        """ Ensure that splitting does not happen within quoted content,
            and that double quotes are removed. """
        assert_equals(['the', 'little brown', 'fox'],
            split_words('the "little brown" fox')
        )

    def test_doubled_quotes_are_ignored_and_singled(self):
        """ Ensure that double quotes are ignored for splitting and
            transformed into single double quotes """
        assert_equals(['the', '"little', 'brown"', 'f"ox'],
            split_words('the ""little brown"" f""ox')
        )

    def test_doubled_quotes_within_quotes_are_ignored_and_singled(self):
        """ Ensure that double quotes within quoted content are ignored 
            for splitting and transformed into single double quotes """
        assert_equals(['the', 'little "brown"', 'fox'],
            split_words('the "little ""brown""" fox')
        )


    def test_split_removes_spaces_not_in_quoted_content(self):
        """ Test that splitting removes spaces unless they are in
            quoted content """
        assert_equals(['the', ' little ', 'brown', 'fox'],
            split_words(' the  " little "  brown  fox ')
        )

    def test_multiple_quoted_sections(self):
        """ Test we can have multiple quoted sections """
        assert_equals(['the', 'little brown', 'fox', 'jumps "over"', 'the', 'dog'],
            split_words('the "little brown" fox "jumps ""over""" the dog')
        )

    def test_quotes_are_balanced(self):
        """ Test that unbalanced quotes add an ending quote """
        unbalanced = 'the "little brown" fox "jumps ""over""" "the dog'
        assert_equals(split_words(unbalanced + '"'), split_words(unbalanced))


class TestParseSortStatement(object):
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
