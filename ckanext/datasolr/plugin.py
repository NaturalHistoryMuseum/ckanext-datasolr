import ckan.plugins as p
import re

from ckanext.datasolr.config import config
from ckanext.datasolr.logic.action import datastore_solr_search


class DataSolrPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IConfigurable)
    p.implements(p.interfaces.IActions)

    # IConfigurable
    def configure(self, ckan_config):
        for name in ckan_config:
            r = re.search('^datasolr\.([-a-fA-F0-9]+)\.([^.]+)$', name)
            if not r:
                continue
            resource_id = r.group(1)
            setting = r.group(2)
            value = ckan_config[name]
            if resource_id not in config:
                config[resource_id] = {}
            config[resource_id][setting] = value

    # IActions
    def get_actions(self):
        return {
            'datastore_solr_search': datastore_solr_search
        }