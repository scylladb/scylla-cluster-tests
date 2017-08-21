import logging
import elasticsearch

logger = logging.getLogger(__name__)


class ES(object):
    """
    Provides interface for Elasticsearch DB
    """

    def __init__(self, *args, **kwargs):
        self._url = kwargs.get('url', 'http://ec2-54-89-144-171.compute-1.amazonaws.com:9200/')
        self._es = elasticsearch.Elasticsearch([self._url])

    def _create_index(self, index):
        self._es.indices.create(index=index, ignore=400)

    def create(self, index, doc_type, doc_id, body):
        """
        Add document in json format
        """
        logger.info('Create doc')
        logger.info('INDEX: %s', index)
        logger.info('DOC_TYPE: %s', doc_type)
        logger.info('DOC_ID: %s', doc_id)
        logger.info('BODY: %s', body)
        self._create_index(index)
        self._es.create(index=index, doc_type=doc_type, id=doc_id, body=body)

    def update(self, index, doc_type, doc_id, body):
        """
        Update document with partial data
        """
        logger.info('Update doc %s with info %s', doc_id, body)
        self._es.update(index=index, doc_type=doc_type, id=doc_id, body=dict(doc=body))

    def get_all(self, index, limit=1000):
        """
        Search for documents for the certain index
        """
        return self._es.search(index=index, size=limit)

    def get_doc(self, index, doc_id, doc_type='_all'):
        """
        Get document by id
        """
        doc = self._es.get(index=index, doc_type=doc_type, id=doc_id, ignore=[400, 404])
        if not doc['found']:
            logger.warning('Document not found: %s %s', doc_id, doc_type)
            return None
        return doc

    def delete(self, index, doc_type, doc_id):
        """
        Delete document
        """
        if self.get_doc(index, doc_id, doc_type):
            self._es.delete(index=index, doc_type=doc_type, id=doc_id)
