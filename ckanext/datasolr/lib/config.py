#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-datasolr
# Created by the Natural History Museum in London, UK

import pylons


def get_datasolr_resources():
    '''Return a dictionary of all datasolr resources, as defined on the
    CKAN Pylons configuration file


    :returns: dict

    '''
    config_key = u'ckanext.datasolr.'
    return {k.replace(config_key, u''): pylons.config.get(k)
            for k in pylons.config.keys() if config_key in k}



