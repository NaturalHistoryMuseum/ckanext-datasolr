import re
import ckanext.datastore.helpers as datastore_helpers
from ckanext.datasolr.lib.solr import Solr


def default_field_mapper(ckan_field_name):
    """ Converts a ckan field name to a solr field name

    This simply strips all characters not in the range [a-zA-Z0-9_]

    @param ckan_field_name: Input field name as provided to ckan apis
    @returns: Solr field name
    """
    return re.sub('[^a-zA-Z0-9_]', '', ckan_field_name)


class SolrQueryApi(object):
    """ Perform an API query via solr and return information to fetch data.

    Given an API datastore_search request, this will perform a Solr query to
    get  matching document ids, and row counts. It will then return the row
    count and a list of ids of the matching rows.

    @param search_url: SOLR url to perform search. This should include the
        request handler, eg. http://localhost:8080/solr/select
    @param solr_id_field: SOLR ID field. Defaults to '_id'
    @param solr_resource_id_field: The field in SOLR that contains the resource id.
        If this is None (the default), then the resource id will not be
        included in the query sent to SOLR - use this if you have a core
        dedicated to one dataset, for which adding the resource_id is
        superfluous.
    @param field_mapper: Calleable (or text path to a calleable) to map field names 
        from API names to SOLR names. If None, default_field_mapper is used.
    """
    def __init__(self, search_url, solr_id_field='_id',
                 solr_resource_id_field=None, field_mapper=None):
        self.solr_resource_id_field = solr_resource_id_field
        if field_mapper is None:
            self.field_mapper = default_field_mapper
        elif isinstance(field_mapper, basestring):
            field_mapper_path = field_mapper.split('.')
            field_mapper_module = import_module('.'.join(field_mapper_path[:-1]))
            self.field_mapper = getattr(field_mapper_module, field_mapper_path[-1])
        else:
            self.field_mapper = field_mapper
        self.solr = Solr(search_url, solr_id_field, 'AND',
                         result_formatter=self._solr_formatter)

    def fetch(self, resource_id, filters=None, q=None, limit=100, offset=0,
              sort=None, distinct=None):
        """ Perform a query and fetch results.

        All field names are translated using the field mapper.

        @param resource_id: The resource id to match from. Note that is sent
            to SOLR only if a solr_resource_id_field was specified in the
            constructor;
        @param filters: Dictionary matching field to search value
        @param q: Either a string (for full text search) or a dictionary of
            field name to value for wildcard searches on individual fields
        @param limit: Number of rows to fetch. Defaults to 100.
        @param offset: Offset to fetch from. Defaults to 0.
        @param sort: SORT statement (eg. fieldName ASC)
        @param distinct: If not None, field on which to group the result
            (returning only one row per value of the field)
        @returns: A tuple (total number of records, [list of ids])
        """
        # Prepare query
        solr_args = {}
        solr_query, solr_values = self._datastore_query_to_solr(filters, q)
        if self.solr_resource_id_field is not None:
            solr_query.append(self.solr_resource_id_field + ':{}')
            solr_values.append(resource_id)
        if sort is None:
            sort = '{} ASC'.format(self.solr.id_field)
        else:
            sort_s = self._parse_sort_statement(sort)
            sort = ', '.join([self.field_mapper(v[0]) + ' ' + v[1] for v in sort_s])
        if distinct:
            solr_args['group'] = 'true'
            solr_args['group.field'] = distinct
            solr_args['group.main'] = 'true'
        solr_args = dict({
            'q': (solr_query, solr_values),
            'start': offset,
            'rows': limit,
            'sort': sort
        }.items() + solr_args.items())
        # Fetch ids from SOLR
        return self.solr.search(**solr_args)

    def _datastore_query_to_solr(self, filters, q):
        """ Transform datastore query parameters into solr query parameters

        @param filters: Dictionary of field name to value (for exact matches)
        @param q: Either a string (for full text search) or a dictionary of
            field name to value for wildcard searches on individual fields
        @returns: A tuple containing the solr query and the replacement values
        """
        solr_query = []
        solr_values = []
        if filters:
            for field in filters:
                if field.startswith('_'):
                    continue
                if isinstance(filters[field], basestring):
                    solr_query.append(self.field_mapper(field) + ':{}')
                    solr_values.append(filters[field])
                else:
                    field_query = []
                    for field_value in filters[field]:
                        field_query.append(self.field_mapper(field) + ':{}')
                        solr_values.append(field_value)
                    solr_query.append('(' + ' OR '.join(field_query) + ')')
        if isinstance(q, basestring):
            words = (w for w in q.split(' ') if w)
            for word in words:
                solr_query.append('_fulltext:{}')
                solr_values.append(word)
        elif q:
            for field in q:
                if q[field].endswith(':*'):
                    value = q[field][:-2]
                else:
                    value = q[field]
                solr_query.append(self.field_mapper(field) + ':*{}*')
                solr_values.append(value)
        if len(solr_query) == 0:
            solr_query.append('*:*')
        return solr_query, solr_values

    def _solr_formatter(self, solr_id_field, documents):
        """ Formatter used to transform a solr result set into a list of ids

        @param solr_id_field: The SOLR id field
        @param documents: List of documents returned by SOLR. Must be at least
            one document.
        @returns: A list of ids
        """
        return [r[solr_id_field] for r in documents]

    def _parse_sort_statement(self, sort):
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


class SolrQueryApiSql(SolrQueryApi):
    """ Perform an API query via solr and return SQL query to fetch data

    This is an implementation of SolrQueryApi that formats the result
    as an SQL query that can be used to fetch the data from Postgresql.

    @param search_url: SOLR url to perform search. This should include the
        request handler, eg. http://localhost:8080/solr/select
    @param id_field: The Postgres field to use to match rows between Solr and
         Postgres. Defaults to '_id'. Double quotes will be stripped out.
    @param solr_id_field: The SOLR field to use to match rows between Solr and
        Postgres. Defaults to '_id'
    @param solr_resource_id_field: The field in SOLR that contains the resource id.
        If this is None (the default), then the resource id will not be
        included in the query sent to SOLR - use this if you have a core
        dedicated to one dataset, for which adding the resource_id is
        superfluous.
    @param field_mapper: Calleable (or text path to a calleable) to map field names 
        from API names to SOLR names. If None, default_field_mapper is used.
    """
    def __init__(self, search_url, id_field='_id',
                 solr_id_field='_id', solr_resource_id_field=None,
                 field_mapper=None):
        super(SolrQueryApiSql, self).__init__(
            search_url, solr_id_field,
            solr_resource_id_field, 
            field_mapper
        )
        self.id_field = id_field

    def fetch(self, resource_id, filters=None, q=None, limit=100, offset=0,
              sort=None, distinct=False, fields=None):
        """ Perform a query, fetch the ids and return an SQL query to fetch
        the data.

        @param resource_id: The resource id to match from. Note that is sent
            to SOLR only if a resource_id_field was specified in the
            constructor. For the query, all characters not in [-a-fA-F0-0]
            will be removed;
        @param filters: Dictionary matching field to search value
        @param q: Either a string (for full text search) or a dictionary of
            field name to value for wildcard searches on individual fields
        @param limit: Number of rows to fetch. Defaults to 100.
        @param offset: Offset to fetch from. Defaults to 0.
        @param sort: SORT statement (eg. fieldName ASC).
        @param distinct: Whether this should be a distinct query or not.
            Distinct queries only work with a single field, an exception
            will be raised if more than one field is being queries;
        @param fields: List of fields to fetch. If None, then '*' is used.
            Double quotes will be stripped out of all field names.

        @returns: A tuple (total number of records, sql query, sql values)
        """
        # Prepare the field list and order statement
        field_list = '*'
        if fields is not None:
            field_list = ','.join([
                '"' + re.sub('"', '', f) + '"' for f in fields
            ])
        order_statement = ''
        if sort:
            sort_s = self._parse_sort_statement(sort)
            sort_s = [(re.sub('"', '', f), o) for f,o in sort_s]
            order_statement = 'ORDER BY {}'.format(
                ''.join('"{}" {}'.format(f,o) for f, o in sort_s)
            )
        # Get the results
        if distinct:
            if fields is None or len(fields) == 0:
                distinct = None
            elif len(fields) == 1:
                distinct = fields[0]
            else:
                raise ValueError('Distinct queries only work with one field')
        results = super(SolrQueryApiSql, self).fetch(
            resource_id, filters, q, limit, offset, sort, distinct
        )
        # Format the query
        sql = results[1][0].format(
            field_list=field_list,
            resource_id=re.sub('[^-a-fA-F0-9]', '', resource_id),
            id_field=re.sub('"', '', self.id_field),
            order_statement=order_statement
        )
        if not datastore_helpers.is_single_statement(sql):
            raise ValueError({
                'query': ['Query is not a single statement.']
            })
        return results[0], sql,  results[1][1]

    def _solr_formatter(self, solr_id_field, documents):
        """ Formatter used to transform a solr result set into an SQL query

        This will always return a query, even for empty result sets, so SQL
        can always be run to get field list.

        @param solr_id_field: The SOLR id field
        @param documents: List of documents returned by SOLR. Must be at least
            one document.
        @returns: A tuple (sql statement, values). The sql statement contains
            four placeholders: {field_list}, {resource_id}, {id_field} and
            {order_statement}
        """
        if len(documents) == 0:
            sql = '''
                SELECT {field_list} FROM "{resource_id}" LIMIT 0
            '''
            values = []
        else:
            values = [r[solr_id_field] for r in documents]
            v_list = '(%s)' + ',(%s)'*(len(documents)-1)
            sql = '''
              SELECT {field_list}
              FROM "{resource_id}"
              WHERE {id_field} = ANY(VALUES
            ''' + v_list + ') {order_statement}'
        return sql, values
