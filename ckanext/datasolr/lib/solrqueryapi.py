import re
from ckanext.datasolr.lib.solr import Solr
from ckanext.datasolr.lib.helpers import is_single_sql_statement, split_words
from importlib import import_module


def default_field_mapper(ckan_field_name):
    """ Converts a ckan field name to a solr field name

    This simply strips all characters not in the range [a-zA-Z0-9_]

    @param ckan_field_name: Input field name as provided to ckan apis
    @returns: Solr field name
    """
    return re.sub('[^a-zA-Z0-9_]', '', ckan_field_name)


class ApiQueryToSolr(object):
    """ Translate an API search into a SOLR query

    Given an API datastore_search request, this will create a SOLR query to
    fetch the matching rows.

    @param field_types: Dictionary of field name to field type for all the fields
        in the resource.
    @param solr_resource_id_field: The field in SOLR that contains the resource id.
        If this is None (the default), then the resource id will not be
        included in the query sent to SOLR - use this if you have a core
        dedicated to one dataset, for which adding the resource_id is
        superfluous.
    @param field_mapper: Calleable (or text path to a calleable) to map field names 
        from API names to SOLR names. If None, default_field_mapper is used.
    """
    def __init__(self, field_types, solr_id_field='_id',
                 solr_resource_id_field=None, field_mapper=None):
        self.field_types = field_types
        self.solr_resource_id_field = solr_resource_id_field
        self.solr_id_field = solr_id_field
        if field_mapper is None:
            self.field_mapper = default_field_mapper
        elif isinstance(field_mapper, basestring):
            field_mapper_path = field_mapper.split('.')
            field_mapper_module = import_module('.'.join(field_mapper_path[:-1]))
            self.field_mapper = getattr(field_mapper_module, field_mapper_path[-1])
        else:
            self.field_mapper = field_mapper

    def validate(self, q, filters):
        """ Remove from the list of filters all the ones we handle

        @param filters: Dictionary of field to value
        @param q: Either a string or a dictionary of field to value
        @returns: Typle (q, filters) with the modified lists
        """
        if isinstance(q, basestring):
            q = None
        for field in self.field_types:
            if filters and field in filters:
                del filters[field]
            if q and field in q:
                del q[field]
        return q, filters

    def build_query(self, resource_id, filters=None, q=None, limit=100, offset=0,
              sort=None, distinct=False):
        """ Build a solr query from API parameters

        @param resource_id: The resource id to match from. Note that is sent
            to SOLR only if a solr_resource_id_field was specified in the
            constructor;
        @param filters: Dictionary matching field to search value
        @param q: Either a string (for full text search) or a dictionary of
            field name to value for wildcard searches on individual fields
        @param limit: Number of rows to fetch. Defaults to 100.
        @param offset: Offset to fetch from. Defaults to 0.
        @param sort: SORT statement as a list of tuples
                     (eg. [('field1', 'ASC'), ('field2', 'DESC'))
        @param distinct: If not False, field on which to group the result
            (returning only one row per value of the field)
        @returns a dictionary defining SOLR request parameters
        """
        solr_args = {}
        solr_query, solr_values = self._datastore_query_to_solr(filters, q)
        if self.solr_resource_id_field is not None:
            solr_query.append(self.solr_resource_id_field + ':{}')
            solr_values.append(resource_id)
        if sort is None:
            sort = '{} ASC'.format(self.solr_id_field)
        else:
            sort = ', '.join([self.field_mapper(v[0]) + ' ' + v[1] for v in sort])
        if distinct:
            solr_args['group'] = 'true'
            solr_args['group.field'] = distinct
            solr_args['group.main'] = 'true'
        return dict({
            'q': (solr_query, solr_values),
            'start': offset,
            'rows': limit,
            'sort': sort
        }.items() + solr_args.items())

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
            for field in self.field_types:
                if field not in filters:
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
            words = split_words(q, quotes=True)
            for word in words:
                solr_query.append('_fulltext:{}')
                solr_values.append(word)
        elif q:
            for field in self.field_types:
                if field not in q:
                    continue
                if q[field].endswith(':*'):
                    value = q[field][:-2]
                else:
                    value = q[field]
                solr_query.append(self.field_mapper(field) + ':*{}*')
                solr_values.append(value)
        if len(solr_query) == 0:
            solr_query.append('*:*')
        return solr_query, solr_values


class SolrQueryToSql(object):
    """ Perform solr query and return SQL query to fetch matching rows in postgres

    @param search_url: SOLR url to perform search. This should include the
        request handler, eg. http://localhost:8080/solr/select
    @param id_field: The Postgres field to use to match rows between Solr and
         Postgres. Defaults to '_id'. Double quotes will be stripped out.
    @param solr_id_field: The SOLR field to use to match rows between Solr and
        Postgres. Defaults to '_id'
    """
    def __init__(self, search_url, id_field='_id', solr_id_field='_id'):
        self.id_field = id_field
        self.solr = Solr(search_url, solr_id_field, 'AND',
                         result_formatter=self._solr_formatter)

    def fetch(self, resource_id, solr_args, fields=None, sort=None):
        """ Perform a query, fetch the ids and return an SQL query to fetch
        the data.

        @param resource_id: The resource id to match from. Note that is sent
            to SOLR only if a resource_id_field was specified in the
            constructor. For the query, all characters not in [-a-fA-F0-0]
            will be removed;
        @param solr_args: Solr query arguments
        @param fields: Fields to return
        @param sort: Sort expression as a list of tuples
                     (eg. [('field1', 'ASC'), ('field2', 'DESC')])

        @returns: A dictionary {
            'total': Total number of records,
            'sql': Sql query,
            'values': Sql replacement values,
            'stats': Solr stats if any were requested, or None
        }
        """
        # Prepare the field list and order statement
        field_list = '*'
        if fields is not None:
            field_list = ','.join([
                '"' + re.sub('"', '', f) + '"' for f in fields
            ])
        order_statement = ''
        if sort:
            sort_s = [(re.sub('"', '', f), o) for f,o in sort]
            order_statement = 'ORDER BY {}'.format(
                ', '.join('"{}" {}'.format(f,o) for f, o in sort_s)
            )
        results = self.solr.search(**solr_args)
        resource_id = re.sub('[^-a-fA-F0-9]', '', resource_id)

        # Awful hack!!!! But only until we switch to elastic search
        resource_id = '{resource_id}" LEFT JOIN gbif.occurrence ON "gbifOccurrenceID" = "occurrenceID'.format(
            resource_id=resource_id
        )
        field_list += ', "gbifIssue", "gbifID"'

        # Format the query
        sql = results['docs'][0].format(
            field_list=field_list,
            resource_id=resource_id,
            id_field=re.sub('"', '', self.id_field),
            order_statement=order_statement
        )
        if not is_single_sql_statement(sql):
            raise ValueError({
                'query': ['Query is not a single statement.']
            })
        return {
            'total': results['total'],
            'sql': sql,
            'values': results['docs'][1],
            'stats': results['stats'],
            'next_cursor': results['next_cursor']
        }

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
              WHERE "{id_field}" = ANY(VALUES
            ''' + v_list + ') {order_statement}'
        return sql, values
