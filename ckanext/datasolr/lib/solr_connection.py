#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-datasolr
# Created by the Natural History Museum in London, UK

import solr

import urllib
import json


class SolrConnection(solr.SolrConnection):
    '''
    Extend solr connection with a schema call
    '''

    # Field cache - keyed by connection URL to prevent clashes
    _fields_cache = {}

    def fields(self):
        # If we haven't already populated the _fields list, build it
        if not self._fields_cache.get(self.url, None):

            self._fields_cache[self.url] = []

            query = {
                u'wt': u'json'
            }
            request = urllib.urlencode(query, doseq=True)

            selector = self.path + '/admin/luke'

            rsp = self._post(selector, request, self.form_headers)
            data = rsp.read()
            solr_schema = json.loads(data)

            for field_name, field in solr_schema[u'fields'].items():

                # Parse schema - ITS--------------. Third character denotes if field is stored
                is_stored = field[u'schema'][2] == u'S'
                is_indexed = field[u'schema'][0] == u'I'

                field_type = field[u'type'].replace(u'field_', u'')
                if field_type == u'string':
                    field_type = u'text'

                # Structure same as the datastore search
                self._fields_cache[self.url].append({
                    u'id': field_name,
                    u'type': field_type,
                    u'indexed': is_indexed,
                    u'stored': is_stored
                })

        return self._fields_cache[self.url]

    def indexed_fields(self):
        '''
        Get all filtered fields
        @return:
        '''
        return [{u'id': f[u'id'], u'type': f[u'type']} for f in self.fields() if f[u'indexed']]

    def stored_fields(self):
        '''
        Get all stored fields
        @return:
        '''
        return [{u'id': f[u'id'], u'type': f[u'type']} for f in self.fields() if f[u'stored']]
