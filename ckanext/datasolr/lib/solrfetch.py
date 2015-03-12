from ckanext.datasolr.lib.solr import Solr

class SolrFetcher(object):
    """ Fetch a datastore result set from psql using SOLR to perform the query

    This will:
    - Perform the query using SOLR and get matching ids;
    - Fetch the actual rows using PostgreSQL;
    - Return the rows and total count.

    @param connection: A database connection object
    @param search_url: SOLR url to perform search. This should include the
        request handler, eg. http://localhost:8080/solr/select
    @param id_field: The PostgreSQL field to use to match rows between Solr and
         PostgreSQL. Defaults to '_id'
    @param solr_id_field: The SOLR field to use to match rows between Solr and
        PostgreSQL. Defaults to '_id'
    @param solr_resource_id_field: The field in SOLR that contains the resource id.
        If this is None (the default), then the resource id will not be
        included in the query sent to SOLR - use this if you have a core
        dedicated to one dataset, for which adding the resource_id is
        superfluous.
    """
    def __init__(self, connection, search_url, id_field='_id',
                 solr_id_field='_id', solr_resource_id_field=None):
        self.connection = connection
        self.id_field = id_field
        self.solr_resource_id_field = solr_resource_id_field
        self.solr = Solr(search_url, solr_id_field, 'AND',
                         result_formatter=self._solr_to_sql)

    def fetch(self, resource_id, filters=None, q=None, limit=100, offset=0,
              sort=None):
        """ Perform a query and fetch results.

        @param resource_id: The resource id to match from. Note that is sent
            to SOLR only if a resource_id_field was specified in the
            constructor;
        @param filters: Dictionary matching field to search value
        @param q: Either a string (for full text search) or a dictionary of
            field name to value for wildcard searches on individual fields
        @param limit: Number of rows to fetch. Defaults to 100.
        @param offset: Offset to fetch from. Defaults to 0.
        @param sort: SORT statement (eg. fieldName ASC)

        @returns: A tuple (total number of records, database result object
                           or None)
        """
        # Prepare query
        solr_query, solr_values = self._datastore_query_to_solr(filters, q)
        if self.solr_resource_id_field is not None:
            solr_query.append(self.solr_resource_id_field + ':{}')
            solr_values.append(resource_id)
        if sort is None:
            sort = '{} ASC'.format(self.solr.id_field)
        # Fetch ids from SOLR
        result = self.solr.search(
            q=(solr_query, solr_values),
            start=offset,
            rows=limit,
            sort=sort
        )
        if result[0] == 0:
            return 0, None

        # Fetch rows from postgres
        sql = result[1][0].format(
            resource_id=resource_id,
            id_field=self.id_field
        )
        values = result[1][1]
        return result[0], self.connection.execute(sql, values)

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
                solr_query.append(field + ':{}')
                solr_values.append(filters[field])
        if isinstance(q, basestring):
            words = (w for w in q.split(' ') if w)
            for word in words:
                solr_query.append('_fulltext:{}')
                solr_values.append(word)
        elif q:
            for field in q:
                solr_query.append(field + ':*{}*')
                solr_values.append(q[field])
        return solr_query, solr_values

    def _solr_to_sql(self, solr_id_field, documents):
        """ Formatter used to transform a solr result set into an SQL query

        @param solr_id_field: The SOLR id field
        @param documents: List of documents returned by SOLR.
        @returns: A tuple (sql statement, values). The sql statement conains
            two placeholders: {resource_id} and {id_field}.
        """
        values = [r[solr_id_field] for r in documents]
        v_list = '(%s)'*len(documents)
        sql = '''
          SELECT *
          FROM "{resource_id}"
          WHERE {id_field} = ANY(VALUES
        ''' + v_list + ')'
        return sql, values
