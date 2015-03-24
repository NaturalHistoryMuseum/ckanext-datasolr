Ckan Datastore Solr extension
=============================

*datasolr* is a [Ckan](http://ckan.org) extension to use [Solr](http://lucene.apache.org/solr) to perform datastore queries.

Motivated by low PostgreSQL performance on very large datasets, *datasolr* provides an alternative API endpoint to perform searches using Solr. *datasolr* is compatible with and can be configured to replace the `datastore_search` API endpoint. The returned results may differ however (see **differences with datastore_search**).

Use case
--------
*datasolr* aims to replace the search component of the datastore only. It is not a full replacement for the datastore, and it's use case is for large datasets that are either not updated, or updated at regular intervals only. As such:

- The data is still stored in (and the actual values fetched from) the PostgreSQL database;
- *datasolr* does not currently provide automatic indexing. Future version may provide on demand batch indexing, however modifying rows as they are updated is not a planned feature.

Differences with datastore_search
---------------------------------
- *datasolr* does not accept double quotes in field names;
- *datsolr* only accepts DISTINCT queries on a single field;
- `datastore_search` allows for PostgreSQL full text query syntax. *datasolr* does not, and does not attempt to parse the PostgreSQL syntax into Solr queries (with the exception of field full text search prefix - see below);
- *datasolr* implements full text search on specific fields differently than the datastore does. While the `q` parameter passed to [datastore_search](http://docs.ckan.org/en/ckan-2.2/datastore.html#ckanext.datastore.logic.action.datastore_search) is typically a full text search string, it can also be a dictionary of field to values - the idea being to implement full text search on individual fields. *datasolr* does not implement this as a full text search, but as a wildcard search instead. Optional PostgreSQL full text query syntax prefix component `:*` is stripped from field full text searches.

Usage
-----

Note that you will need some good understanding of Solr to use this extension. Out-of-the box, *datasolr* does not provide schemaless or dynamic field mapping into Solr - as such, you need to write a [schema](http://www.solrtutorial.com/schema-xml.html) for the specific dataset on which you wish to use *datasolr*.

Note that  *datasolr* can be extended to provide schemaless and/or dynamic field mapping, and future versions may provide this.

For now the typical usage would be:

1. Identify datasets that would benefit from faster searches;
2. Create a schema for that particular dataset (either on a shared core or dedicated core). The field names should be the same as the database field names. Not that Solr fields only support alphanumeric and underscore characters - if your dataset does not conform to this, you will need to provide a custom field mapper (see **configuration**);
3. Index your dataset (see **indexing with data import**)
4. Install and configure *datasolr*.

Configuration
-------------
*datasolr* is configured in the main Ckan configuration file. You first want to add *datasolr* to your list of plugins (such that it is the first plugin to use the IDataSolr interface), and you can then configure it with the following keys (note that most of them are not necessary - the defaults are sensible):

```ini
# Whether to replace datastore_search api calls or not. Set this to False 
# (the default) until you're happy datasolr is working.
datasolr.replace_datastore_search = False

# The action to fall back to when a given resource is not handled by datasolr.
# The default is the main ckan datastore_search action. Unless you're using
# another plugin that overrides this (eg. ckanext-dataproxy) you do not need
# to change this.
datasolr.fallback = ckanext.datastore.logic.action.datastore_search

##
# Below are the parameters used by all queries that do not have a resource
# specific configuration. Typically, unless you implement dynamic field
# mapping, you would only want resource specific configuration.
##

# The method used to map API field names to Solr field names. The default 
# implementation strips characters that are not allowed as Solr field names.
# Use this if you have non alphanumeric characters in your field names. This
# could also be used to provide dynamic field mapping in Solr.
datasolr.field_mapper = ckanext.datasolr.lib.solrqueryapi.default_field_mapper

# The Solr search url, including the core and searcher
datasolr.search_url = http://localhost:8080/solr/collection2/select

# A unique field in the dataset. This defaults to `_id`, which is what
# the datastore uses as primary key. You may use this if your `_id` is not 
# consistent across rebuilds, and you'd rather use a different field.
datasolr.id_field = _id

# The field in the Solr schema that matches the key defined in id_field
datasolr.solr_id_field = _id

# The field in the Solr schema that holds the resource id. If this is present,
# then the resource id will be included in all queries. This means you can use
# a single Solr core for multiple datasets. If you dedicate a core to a
# single dataset, then you might as well omit this.
datasolr.resource_id_field = resource_id

##
# Resource specific configuration contain the same fields, prefixed
# by `resource.<resource id>`.
##

datasolr.resource.75cc58ff-db88-4ca7-a321-9bb24a89b781.field_mapper = ckanext.datasolr.lib.solrqueryapi.default_field_mapper
datasolr.resource.75cc58ff-db88-4ca7-a321-9bb24a89b781.search_url = http://localhost:8080/solr/collection2/select
datasolr.resource.75cc58ff-db88-4ca7-a321-9bb24a89b781.id_field = _id
datasolr.resource.75cc58ff-db88-4ca7-a321-9bb24a89b781.solr_id_field = _id
datasolr.resource.75cc58ff-db88-4ca7-a321-9bb24a89b781.resource_id_field = resource_id
```

Extending *datasolr*
--------------------
The field mapper, allowing users to implement different field mapping strategies such as dynamic fields, can be set directly in the configuration (see **configuration**).

It is also possible to extend how *datasolr* builds queries by implementing the `IDataSolr` interface, which is analogous to the the `IDatastore` interace. The `IDataSolr` interface is documented in the [source code](https://github.com/NaturalHistoryMuseum/ckanext-datasolr/blob/master/ckanext/datasolr/interfaces.py).

Here is an example implementation that adds a custom filter to return all rows which have a value for `image_url`:

```python
import ckan.plugins as p
from ckanext.datasolr.interfaces import IDataSolr

class MyPlugin(p.SingletonPlugin):
    p.implements(IDataSolr)

    def datasolr_validate(self, context, data_dict, field_types):
        """ Validate the query by removing all filters that we manage """
        if 'filters' in data_dict and '_has_image' in data_dict['filters']:
            del data_dict['filters']['_has_image']

    def datasolr_search(self, context, data_dict, field_types, query_dict):
        """ Add our custom search terms """
        if 'filters' in data_dict and '_has_image' in data_dict['filters']:
            query_dict['q'][0].append('image_url:[* TO *]')
        return query_dict
```


Indexing with data import
-------------------------
Solr offers a way to index data directly from a PostgreSQL database using the [Data Import Request Handler](http://wiki.apache.org/solr/DataImportHandler) module.

To index your resources in this way you will need to:
- Add the PostgreSQL JDBC driver to your Solr installation;
- Add the solr dataimport handler jar to your Solr installation;
- Add a data import handler section in your `schema.xml`, for instance:

    ```xml
    <requestHandler name="/dataimport" class="org.apache.solr.handler.dataimport.DataImportHandler">
        <lst name="defaults">
            <str name="config">data-config.xml</str>
        </lst>
    </requestHandler>
    ```
- Add a `data-config.xml` file to describe how to import the resource to index, for instance:

    ```xml
        <dataConfig>
            <dataSource driver="org.postgresql.Driver"
                        url="jdbc:postgresql://my_postgres_server:5432/datastore_default"
                        user="datastore_default"
                        password="my_secret_password" />
            <document name="my_dataset">
                <entity name="my_entity"
                    query="
                        SELECT  &quot;_id&quot;, &quot;myfield&quot;, &quot;my_other_field&quot;
                        FROM &quot;my_resource_id&quot; ORDER BY _id ASC"
                >
                </entity>
            </document>
        </dataConfig>
    ```