import ckan.plugins as p
from pylons import config as pylons_config
import re

from ckanext.datasolr.config import config
from ckanext.datasolr.logic.action import datastore_solr_search


class DataSolrPlugin(p.SingletonPlugin):
    p.implements(p.interfaces.IConfigurable)
    p.implements(p.interfaces.IActions)
    p.implements(p.IRoutes, inherit=True)

    # IConfigurable
    def configure(self, ckan_config):
        prefix = 'datasolr.'
        config_items = ['replace_datastore_search', 'fallback',
                        'search_url', 'id_field', 'solr_id_field',
                        'solr_resource_id_field']
        for long_name in ckan_config:
            if not long_name.startswith(prefix):
                continue
            name = long_name[len(prefix):]
            if name in config_items:
                config[name] = ckan_config[long_name]
            elif name.startswith('resource.'):
                r = re.search('^resource\.([-a-fA-F0-9]+)\.([^.]+)$', name)
                if not r:
                    continue
                resource_id = r.group(1)
                setting = r.group(2)
                value = ckan_config[long_name]
                if resource_id not in config['resources']:
                    config['resources'][resource_id] = config['resources']['_defaults']
                config['resources'][resource_id][setting] = value

    # IActions
    def get_actions(self):
        return {
            'datastore_solr_search': datastore_solr_search
        }

    # IRoutes
    def before_map(self, map):
        # configure hasn't been invoked yet, so read off the pylons config.
        replace = pylons_config.get('datasolr.replace_datastore_search', 'False')
        if replace.lower() in ['yes', 'ok', 'true', '1']:
            map.connect(
                'datasolr',
                '/api/3/action/datastore_search',
                controller='api',
                action='action',
                logic_function='datastore_solr_search',
                ver=u'/3'
            )
        return map
