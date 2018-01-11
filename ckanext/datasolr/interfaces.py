#!/usr/bin/env python
# encoding: utf-8
#
# This file is part of ckanext-datasolr
# Created by the Natural History Museum in London, UK

import ckan.plugins.interfaces as interfaces


class IDataSolr(interfaces.Interface):
    '''Allow modifying DataSolr queries'''

    def datasolr_validate(self, context, data_dict, fields_types):
        '''Validates the ``data_dict`` sent by the user

        This is analogous to IDatastore.datastore_validate.

        This is the first method that's called. It's used to guarantee that
        there aren't any unrecognized parameters, so other methods don't need
        to worry about that.

        You'll need to go through the received ``data_dict`` and remove
        everything that you understand as valid. For example, if your extension
        supports an ``age_between`` filter, you have to remove this filter from
        the filters on the ``data_dict``.

        The same ``data_dict`` will be passed to every IDataSolr extension in
        the order they've been loaded (the ``datasolr`` plugin will always
        come first). One extension will get the resulting ``data_dict`` from
        the previous extensions. In the end, if the ``data_dict`` is empty, it
        means that it's valid. If not, it's invalid and we throw an error.

        Attributes on the ``data_dict`` that can be comma-separated strings
        (e.g. fields) will already be converted to lists. 'sort' is converted
        to a list of tuples, where the first element is the field name and
        the second element the sort order.

        @param context: the context
        @param data_dict: the parameters received from the user
        @param fields_types: the current resource's fields as dict keys and
            their types as values
        '''
        return data_dict

    def datasolr_search(self, context, data_dict, fields_types, query_dict):
        '''Modify queries made on datastore_solr_search

        This is analogous to IDatastore.datastore_search.

        The overall design is that every IDataSolr extension will receive the
        ``query_dict`` with the modifications made by previous extensions, then
        it can add/remove stuff into it before passing it on. You can think of
        it as pipes, where the ``query_dict`` is being passed to each
        IDataSolr extension in the order they've been loaded allowing them to
        change the ``query_dict``. The ``datasolr`` extension always comes
        first.

        The ``query_dict`` represents the search parameters that will be
        sent to Solr. For instance:
        {
            'q': (['field1:{}', 'field2:{}'], ['value 1', 'value 2']),
            'start': 0,
            'rows': 0,
            'sort': 'field1',
            'group': 'true',
            'group.field': 'field1', 
        }

        The ``q`` key is a special case. It is a tuple containing two elements.
        The first element is the query itself, which may contain '{}' style
        placeholders. The second element is a list of values, which are
        escaped and then inserted into the query. The query itself can be
        a list, in which case it's terms are ANDed together (before the
        replacements are applied). So in the example above, the final
        query sent to solr is:

            field1:value\ 1 AND field2:value\ 2

        After finishing this, you should return your modified ``query_dict``.

        :param context: the context
        :type context: dictionary
        :param data_dict: the parameters received from the user
        :type data_dict: dictionary
        :param fields_types: the current resource's fields as dict keys and
            their types as values
        :type fields_types: dictionary
        :param query_dict: the current query_dict, as changed by the IDatastore
            extensions that ran before yours
        :type query_dict: dictionary

        :returns: the query_dict with your modifications
        :rtype: dictionary
        '''
        return query_dict

