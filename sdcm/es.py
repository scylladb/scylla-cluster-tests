import logging
from functools import cached_property

import elasticsearch

from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)


class ES(elasticsearch.Elasticsearch):
    """
    Provides interface for Elasticsearch DB
    """

    def __init__(self, _transport=None):
        super().__init__(_transport=_transport, **self.conf)

    @cached_property
    def conf(self):
        return KeyStore().get_elasticsearch_token()

    def _create_index(self, index):
        self.indices.create(index=index, ignore=400)

    def create_doc(self, index, doc_id, body):
        """
        Add document in json format
        """
        LOGGER.info('Create doc')
        LOGGER.info('INDEX: %s', index)
        LOGGER.info('DOC_ID: %s', doc_id)
        LOGGER.info('BODY: %s', body)
        self._create_index(index)
        if self.exists(index=index, id=doc_id):
            self.update(index=index, id=doc_id, body={'doc': body})
        else:
            self.create(index=index, id=doc_id, body=body)

    def update_doc(self, index, doc_id, body):
        """
        Update document with partial data
        """
        LOGGER.info('Update doc %s with info %s', doc_id, body)
        self.update(index=index, id=doc_id, body=dict(doc=body))

    def get_all(self, index, limit=1000):
        """
        Search for documents for the certain index
        """
        return self.search(index=index, size=limit)

    def get_doc(self, index, doc_id):
        """
        Get document by id
        """
        doc = self.get(index=index, id=doc_id, ignore=[
                       400, 404])
        if not doc['found']:
            LOGGER.warning('Document not found: %s', doc_id)
            return None
        return doc

    def delete_doc(self, index, doc_id):
        """
        Delete document
        """
        if self.get_doc(index, doc_id):
            self.delete(index=index, id=doc_id)
