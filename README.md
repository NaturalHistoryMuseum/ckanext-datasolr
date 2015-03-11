Ckan Datastore Solr extension
=============================

*datasolr* is a Ckan extension to use [Solr](http://lucene.apache.org/solr) to perform datastore queries.

Motivated by low PostgreSQL performance on very large datasets, *datasolr* provides an alternative API endpoint to perform searches using Solr. *datasolr* is compatible with and can be configured to replace the `datastore_search` API endpoint. The returned results may differ however (see notes below).

Notes:

- The data is still stored (and the actual values fetched from) the PostgreSQL database. A case could (easily) be made for using a key/value store rather than PostgreSQL for storing datasets, however this would be a complete replacement of the datastore extension. *datasolr* only replaces the search component by using Solr to perform the searches;
- *datasolr* does not currently provide automatic indexing. Future version may provide on demand batch indexing, however modifying rows as they are updated is not a planned feature (again this would require larger modifications of the ckan datastore). The use case of *datasolr* is for large datasets that are either not updated, or batch updated at regular intervals.
- *datasolr* full text search on fields does not use stemmed words, so the results may differ.