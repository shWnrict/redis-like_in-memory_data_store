# src/datatypes/documents.py
from src.logger import setup_logger
import threading
import json

logger = setup_logger("documents")

class Documents:
    def __init__(self):
        self.lock = threading.Lock()

    def _validate_json(self, json_string):
        try:
            return json.loads(json_string)
        except json.JSONDecodeError:
            return None

    def insert(self, store, key, document):
        """
        Insert a document into the database.
        """
        with self.lock:
            if key not in store:
                store[key] = []
            if not isinstance(store[key], list):
                return "ERR Key is not a document collection"

            document_data = self._validate_json(document)
            if document_data is None:
                return "ERR Invalid JSON document"

            store[key].append(document_data)
            logger.info(f"INSERT {key} -> {document_data}")
            return "OK"

    def find(self, store, key, query):
        """
        Find documents in a collection that match the query.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list):
                return "ERR Collection does not exist"

            query_data = self._validate_json(query)
            if query_data is None:
                return "ERR Invalid JSON query"

            result = [doc for doc in store[key] if self._matches_query(doc, query_data)]
            logger.info(f"FIND {key} -> {result}")
            return result

    def update(self, store, key, query, update_fields):
        """
        Update documents in a collection based on a query.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list):
                return "ERR Collection does not exist"

            query_data = self._validate_json(query)
            update_data = self._validate_json(update_fields)

            if query_data is None or update_data is None:
                return "ERR Invalid JSON"

            updated_count = 0
            for doc in store[key]:
                if self._matches_query(doc, query_data):
                    doc.update(update_data)
                    updated_count += 1

            logger.info(f"UPDATE {key} -> {updated_count} documents updated")
            return updated_count

    def delete(self, store, key, query):
        """
        Delete documents in a collection that match the query.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list):
                return "ERR Collection does not exist"

            query_data = self._validate_json(query)
            if query_data is None:
                return "ERR Invalid JSON query"

            initial_count = len(store[key])
            store[key] = [doc for doc in store[key] if not self._matches_query(doc, query_data)]
            deleted_count = initial_count - len(store[key])

            logger.info(f"DELETE {key} -> {deleted_count} documents deleted")
            return deleted_count

    def aggregate(self, store, key, operation, field):
        """
        Perform an aggregation operation on a specific field.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list):
                return "ERR Collection does not exist"

            if operation.lower() == "count":
                result = len(store[key])
            elif operation.lower() == "sum":
                result = sum(doc.get(field, 0) for doc in store[key] if isinstance(doc.get(field), (int, float)))
            elif operation.lower() == "avg":
                valid_values = [doc.get(field, 0) for doc in store[key] if isinstance(doc.get(field), (int, float))]
                result = sum(valid_values) / len(valid_values) if valid_values else 0
            else:
                return "ERR Unknown operation"

            logger.info(f"AGGREGATE {key} {operation.upper()} {field} -> {result}")
            return result

    def _matches_query(self, document, query):
        """
        Helper method to match a document against a query.
        """
        for key, value in query.items():
            if key not in document or document[key] != value:
                return False
        return True
