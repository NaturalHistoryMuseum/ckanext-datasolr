import re


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