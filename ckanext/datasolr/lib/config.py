import pylons


def get_datasolr_resources():
    """
    Return a dictionary of all datasolr resources, as defined on the
    CKAN Pylons configuration file
    @return: dict
    """
    config_key = 'ckanext.datasolr.'
    return {k.replace(config_key, ''): pylons.config.get(k)
            for k in pylons.config.keys() if config_key in k}


def is_datasolr_resource(resource_id):
    """
    Is a solr resource id in the list of datasolr resources
    @param resource_id:
    @return:
    """
    return resource_id in get_datasolr_resources()
