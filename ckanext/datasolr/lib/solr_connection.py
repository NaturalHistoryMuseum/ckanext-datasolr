#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '10/08/2017'.
"""

import solr

import urllib
import json


class SolrConnection(solr.SolrConnection):
    """
    Extend solr connection with a schema call
    """

    # Field cache - keyed by connection URL to prevent clashes
    _fields_cache = {}

    def fields(self):
        # If we haven't already populated the _fields list, build it
        if not self._fields_cache.get(self.url, None):

            self._fields_cache[self.url] = []

            query = {
                'wt': 'json'
            }
            request = urllib.urlencode(query, doseq=True)

            selector = self.path + '/admin/luke'

            rsp = self._post(selector, request, self.form_headers)
            data = rsp.read()
            solr_schema = json.loads(data)

            for field_name, field in solr_schema['fields'].items():

                # Parse schema - ITS--------------. Third character denotes if field is stored
                is_stored = field['schema'][2] == 'S'
                is_indexed = field['schema'][0] == 'I'

                field_type = field['type'].replace('field_', '')
                if field_type == 'string':
                    field_type = 'text'

                # Structure same as the datastore search
                self._fields_cache[self.url].append({
                    'id': field_name,
                    'type': field_type,
                    'indexed': is_indexed,
                    'stored': is_stored
                })

        return self._fields_cache[self.url]

    def indexed_fields(self):
        """
        Get all filtered fields
        @return:
        """
        return [{'id': f['id'], 'type': f['type']} for f in self.fields() if f['indexed']]

    def stored_fields(self):
        """
        Get all stored fields
        @return:
        """
        return [{'id': f['id'], 'type': f['type']} for f in self.fields() if f['stored']]
