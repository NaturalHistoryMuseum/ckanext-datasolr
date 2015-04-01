import re
import sqlparse


def split_words(phrase, quotes=True):
    """ Split a phrase into words

    This will optionally keep "quoted terms" as a
    single word, removing the double quotes.

    Double quotes can be escaped by doubling them
    (ie. ""), and they will be singled in the result.
    (though a word that consists only of a
    doubled double quote is removed), and if the
    number of double quotes is not balanced, then an 
    additional double quote is added at the end of the
    phrase.

    @param phrase: Phrase to split
    @param quotes: If True, then statements
        between double quotes are treated as
        a single word, and the quote symbol is
        removed. If False, quotes are ignored.
    """
    if not quotes:
       return [w for w in phrase.split(' ') if w]
    else:
        nb_q = len(re.sub('[^"]', '', phrase))
        if nb_q%2 == 1:
            phrase += '"'
        parts = re.split(' (?=(?:[^"]|"[^"]*")*$)', phrase)
        words = []
        for w in parts:
            if w.endswith('"'):
                w = w[:-1]
            if w.startswith('"'):
                w = w[1:]
            w = w.replace('""', '"')
            if w:
                words.append(w)
        return words

def parse_sort_statement(sort):
    """ Parse input sort statement

    Parse a ckan API sort statement of the type "field1 ASC, field2, field3 DESC"
    into a list of tupples [('field1', 'ASC'), ('field2', 'ASC'), ('field3', 'DESC')]
    We assume the sort statement may contain double quotes around field
    names (and field names may contain double quotes, which are escaped
    by doubling them). The ckan api is not clear on this topic, but since
    ckan field names are allowed to include commas, this seems logical.

    Note that this will strip enclosing double quotes, but will not
    apply the field mapper to field names.

    @param sort: Sort statement
    @returns: list of tuples [(field, 'ASC' or 'DESC'), ...]
    """
    statements = re.split(',(?=(?:[^"]|"[^"]*")*$)', sort)
    order_statements = []
    for statement in statements:
        statement = statement.strip()
        m = re.search('^"?(.+?)"?(?:\s+(ASC|DESC))?$', statement, re.IGNORECASE)
        if not m:
            raise ValueError('Could not parse sort statement')
        field = m.group(1)
        if m.group(2):
            order = m.group(2)
        else:
            order = 'ASC'
        order_statements.append((field, order))
    return order_statements


def is_single_sql_statement(sql):
    """ Check if the given SQL is a single statement or not

    @param sql: SQL
    @returns: True if the SQL is a single statement, false if not
    """
    return len(sqlparse.split(sql)) <= 1