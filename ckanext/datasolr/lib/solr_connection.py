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
    # TODO: Cache this?

    _fields = []

    def indexed_fields(self):

    def stored_fields(self):


    def fields(self, indexed_only=False):
        """
        Return the SOLR index schema - performs a luke request
        @return:
        """
        query = {
            'wt': 'json'
        }
        request = urllib.urlencode(query, doseq=True)

        selector = self.path + '/admin/luke'

        rsp = self._post(selector, request, self.form_headers)
        data = rsp.read()
        solr_schema = json.loads(data)
        fields = []
        for field_name, field in solr_schema['fields'].items():

            # Parse schema - ITS--------------. Third character denotes if field is stored
            is_stored = field['schema'][2] == 'S'
            is_indexed = field['schema'][0] == 'I'

            # If we only want indexed fields and it's not indexed, skip field
            # Or it's not stored / a SOLR internal,  skip the field
            if (indexed_only and not is_indexed) or not is_stored or field_name == '_version_':
                continue

            field_type = field['type'].replace('field_', '')
            if field_type == 'string':
                field_type = 'text'

            # Structure same as the datastore search
            fields.append({
                'id': field_name,
                'type': field_type
            })
        return fields
