import ckanext.datastore.db as db


class Connection(object):
    """ This is an abstraction of the database layer over SqlAlchemy

    The point of this class is to make testing easier - mocking/dependency
    injection are not simple with SqlAlchemy. Given that we only use a
    small and well defined subset of SQLAlchemy's features, an abstraction
    makes is simpler to write our tests.

    @param connection: SqlAlchemy connection object
    """
    def __init__(self, connection):
        self.connection = connection

    def get_fields(self, table):
        """ Return the list of fields & type for the given resource id

        TODO: can/should we cache this?

        @param table: The table from which we want the fields
        @returns: Dict of field name to type
        """
        fields = {}
        all_fields = self.connection.execute(
            u'SELECT * FROM "{0}" LIMIT 1'.format(table)
        )
        for field in all_fields.cursor.description:
            if field[0] == '_id' or not field[0].startswith('_'):
                name = field[0].decode('utf-8')
                field_type = db._get_type(
                    {'connection':self.connection}, field[1]
                )
                fields[name] = field_type
        return fields

    def resolve_alias(self, table):
        """ Return the source name of the given table.

        Note that this relies on CKAN's _table_metadata view.

        @param table: The table name to resolve
        @raises ValueError: If the table does not exist
        @returns: The source table name
        """
        results = self.connection.execute("""
            SELECT alias_of
            FROM   "_table_metadata"
            WHERE name = %s
        """, (table,))
        row = results.first()
        if row is None:
            raise ValueError()
        if row['alias_of']:
            return row['alias_of']
        else:
            return table

    def convert(self, value, field_type):
        """ Convert a given field according to a given type

        @param value: The value to convert
        @param field_type: The type
        """

        try:
            return db.convert(value, field_type)
        except UnicodeDecodeError:
            print(value)
            return ''

    def execute(self, sql, replacements, row_formatter=None):
        """ Executes an sql query

        @param sql: The sql statement
        @param replacements: The replacement values
        @param row_formatter: Function to format row results. If None, then
            each row is returned as a dict of field to value.
        @returns: An array of rows
        """
        results = self.connection.execute(sql, replacements)
        if row_formatter:
            records = [row_formatter(row) for row in results]
        else:
            records = [row for row in results]
        return records

