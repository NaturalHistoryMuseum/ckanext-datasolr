#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-datasolr
# Created by the Natural History Museum in London, UK

from ckan.plugins.toolkit import config


def get_datasolr_resources():
    '''Return a dictionary of all datasolr resources, as defined in the
    CKAN configuration


    :returns: a dictionary of all datasolr resources in the config

    '''
    config_key = u'ckanext.datasolr.'
    return {k.replace(config_key, u''): config.get(k) for k in config.keys() if
            config_key in k}
