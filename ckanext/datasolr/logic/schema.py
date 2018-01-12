#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-datasolr
# Created by the Natural History Museum in London, UK

import ckan.plugins as p
from ckanext.datastore.logic.schema import datastore_search_schema as ckan_datastore_search_schema
from ckanext.datastore.logic.schema import list_of_strings_or_string, json_validator

get_validator = p.toolkit.get_validator

ignore_missing = get_validator(u'ignore_missing')
int_validator = get_validator(u'int_validator')
bool_validator = get_validator(u'boolean_validator')


def datastore_search_schema():
    '''Override the default ckan datastore_search_schema
    To add a cursor for better SOLR searching
    :return: schema


    '''
    schema = ckan_datastore_search_schema()
    # Optional SOLR cursor parameter
    schema[u'cursor'] = [ignore_missing]
    # Optional facets parameter
    schema[u'facets'] = [ignore_missing, list_of_strings_or_string]
    # Optional number of facets to return
    schema[u'facets_limit'] = [ignore_missing, int_validator]
    schema[u'facets_field_limit'] = [ignore_missing, json_validator]
    schema[u'indexed_only'] = [ignore_missing, bool_validator]
    return schema
