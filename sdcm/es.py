import logging
import math
from functools import cached_property

import elasticsearch

from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)


def sanitize_elasticsearch_data(data):
    """
    Recursively sanitize data for Elasticsearch by replacing NaN and Inf values with None.

    Elasticsearch cannot parse NaN and Infinity values as they are not valid in JSON standard.
    This function replaces them with None to prevent mapper_parsing_exception errors.

    Args:
        data: Data structure (dict, list, or primitive) to sanitize

    Returns:
        Sanitized data with NaN/Inf replaced by None
    """
    if isinstance(data, dict):
        return {key: sanitize_elasticsearch_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [sanitize_elasticsearch_data(item) for item in data]
    elif isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return None
        return data
    else:
        return data


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
        LOGGER.info("Create doc")
        LOGGER.info("INDEX: %s", index)
        LOGGER.info("DOC_ID: %s", doc_id)
        LOGGER.info("BODY: %s", body)
        body = sanitize_elasticsearch_data(body)
        self._create_index(index)
        if self.exists(index=index, id=doc_id):
            self.update(index=index, id=doc_id, body={"doc": body})
        else:
            self.create(index=index, id=doc_id, body=body)

    def update_doc(self, index, doc_id, body):
        """
        Update document with partial data
        """
        LOGGER.info("Update doc %s with info %s", doc_id, body)
        body = sanitize_elasticsearch_data(body)
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
        doc = self.get(index=index, id=doc_id, ignore=[400, 404])
        if not doc["found"]:
            LOGGER.warning("Document not found: %s", doc_id)
            return None
        return doc

    def delete_doc(self, index, doc_id):
        """
        Delete document
        """
        if self.get_doc(index, doc_id):
            self.delete(index=index, id=doc_id)
