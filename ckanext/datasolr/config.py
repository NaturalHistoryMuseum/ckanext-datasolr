config = {
    'replace_datastore_search': False,
    'fallback': 'ckanext.datastore.logic.action.datastore_search',
    'field_mapper': None,
    'search_url': None,
    'id_field': '_id',
    'solr_id_field': '_id',
    'solr_resource_id_field': 'resource_id',
    'resources': {
        '_defaults': {
            'field_mapper': None,
            'id_field': '_id',
            'solr_id_field': '_id',
            'solr_resource_id_field': None
        }
    }
}
