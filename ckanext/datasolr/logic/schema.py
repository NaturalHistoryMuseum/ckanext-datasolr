#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""


import ckan.plugins as p
from ckanext.datastore.logic.schema import datastore_search_schema as ckan_datastore_search_schema
from ckanext.datastore.logic.schema import list_of_strings_or_string

get_validator = p.toolkit.get_validator

ignore_missing = get_validator('ignore_missing')


def datastore_search_schema():
    """
    Override the default ckan datastore_search_schema
    To add a cursor for better SOLR searching
    :return: schema
    """
    schema = ckan_datastore_search_schema()
    # Optional SOLR cursor parameter
    schema['cursor'] = [ignore_missing]
    # Optional facets parameter
    schema['facets'] = [ignore_missing, list_of_strings_or_string]
    return schema
