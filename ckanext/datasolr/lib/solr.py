import urllib
import urllib2
from urlparse import urlparse, urlunparse
try:
    import ujson as json
except ImportError:
    import json

class Solr(object):
    """ Low level SOLR search abstraction class.

    This is a class to perform searches, and return id of matching documents.
    It expects a primary key type id field on the data, and returns only that.

    Example usage:
        solr = Solr('http://localhost:8080/solr/specimen_collection/select')
        (num_rows, result_ids) = solr.search(q=(
            ['_fulltext:{}', 'scientificName:{}'],
            ['himalaya', 'carrot']
        ))

    @param search_url: Url to use to perform the search. This should include
        the core url and the search handler, eg. http://localhost/solr/select.
        Query string and fragments are stripped out.
    @param id_field: The id field to return. Defaults '_id'
    @param query_type: Type of query to run when the query is provided as an
        array. Defaults to 'AND'.
    """
    def __init__(self, search_url, id_field='_id', query_type='AND'):
        url = urlparse(search_url)
        self.search_url = urlunparse((
            url.scheme, url.netloc, url.path, url.params, '', ''
        ))
        self.id_field = id_field
        self.query_type = ' ' + query_type.strip() + ' '

    def search(self, **query):
        """ Perform a search and return the results

        All named arguments are used directly SOLR arguments. Note that:
        - 'wt' is always set to 'json'
        - 'fl' is always set to the configured id field.

        The 'q' parameter may be one of:
        - A SOLR search string, which is sent as-is without escaping;
        - A tuple defining (search string or list, list of replacement values)

          If the first element is a list then the search string is build by
          joining the list with the defined query type operator ('AND' or 'OR').

          The replacement values are escaped and inserted into the search string
          using {} type placeholders. This means that non-placeholder {}
          symbols in the search string must be doubled.

        - A list defining (search string, value one, value two, ...)
          where 'search string' is a SOLR search string  containing
          {} placeholders which get replaced by the corresponding value
          (after escaping it for SOLR syntax).

          The search string itself can be a list, which will then get
          joined with the defined query type operator ('AND' or 'OR')
          before the replacement are inserted.

        @returns: (number of results, [list of id field]) tuple
        """
        query['wt'] = 'json'
        query['fl'] = self.id_field
        if 'q' in query and not isinstance(query['q'], basestring):
            base = query['q'][0]
            if not isinstance(base, basestring):
                base = self.query_type.join(base)
            query['q'] = base.format(*[self.escape(t) for t in query['q'][1]])
        resp = urllib2.urlopen(self.search_url + '?' + urllib.urlencode(query))
        data = json.loads(resp.read())
        resp.close()
        # Despite having to unpack the data in this way, tests have shown this
        # to be faster (when combined with the ujson parser) than alternative
        # solr return formats.
        result_count = data['response']['numFound']
        result_list = [r[self.id_field] for r in data['response']['docs']]
        return result_count, result_list

    def escape(self, q):
        """ Escape a query term for solr searches

        @param q: String to escape
        @returns: Escaped string
        """
        q = q.replace('\\', r'\\')
        return ''.join([n for n in self._escape_seq(q)])

    def _escape_seq(self, q):
        """ Helper function to escape solr query terms

         This implementation provides better performance than simple string
         replace. See http://opensourceconnections.com/blog/2013/01/17/escaping-solr-query-characters-in-python/

         @q: Query term
         @yields: List of escaped strings to form the final string
        """
        escape_chars = {
            '+': r'\+', '-': r'\-', '&': r'\&', '|': r'\|', '!': r'\!',
            '(': r'\(', ')': r'\)', '{': r'\{', '}': r'\}', '[': r'\[',
            ']': r'\]', '^': r'\^', '~': r'\~', '*': r'\*', '?': r'\?',
            ':': r'\:', '"': r'\"', ';': r'\;', ' ': r'\ ', '/': r'\/'
        }
        for c in q:
            if c in escape_chars:
                yield escape_chars[c]
            else:
                yield c
