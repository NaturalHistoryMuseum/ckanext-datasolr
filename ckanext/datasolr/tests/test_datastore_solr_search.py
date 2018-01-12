#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-datasolr
# Created by the Natural History Museum in London, UK

import ckan.plugins as p
import copy
import urllib
import json

from ckanext.datasolr.lib.solr_connection import SolrConnection


def main():
    ''' '''
    conn = SolrConnection(u'http://10.11.20.12/solr/specimen_collection')

    # query = {
    #     'wt': 'json'
    # }
    #
    # request = urllib.urlencode(query, doseq=True)
    # # conn = self.conn
    #
    # selector = conn.path + '/admin/luke'
    # rsp = conn._post(selector, request, conn.form_headers)
    # data = rsp.read()
    # solr_schema = json.loads(data)
    # schema = {
    #     'fields': []
    # }
    # for field_name, field in solr_schema['fields'].items():
    #     if field['index'] != '(unstored field)' and field_name != '_version_':
    #         schema['fields'].append({
    #             'id': field_name,
    #             'type': field['type'].replace('field_', '')
    #         })
    # print(schema)
        # print(field_name)

    # print(solr_schema['fields'])

    # c.raw_query(q='_full_text: "hey"')

    # solr_search = SolrDatasetSearch('http://10.11.20.12/solr/specimen_collection', {}, {})
    # solr_search.validate()
    # return solr_search.fetch()



if __name__ == u'__main__':
    main()


