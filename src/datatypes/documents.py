# src/datatypes/documents.py
from src.logger import setup_logger
import threading
import json

logger = setup_logger("documents")

class Documents:
    def __init__(self):
        self.lock = threading.Lock()

    def insert(self, store, key, document):
        """
        Insert a document into the database.
        """
        with self.lock:
            if key not in store:
                store[key] = []
            if not isinstance(store[key], list):
                return "ERR Key is not a document collection"

            try:
                document = json.loads(document)
            except json.JSONDecodeError:
                return "ERR Invalid JSON document"

            store[key].append(document)
            logger.info(f"INSERT {key} -> {document}")
            return "OK"

    def find(self, store, key, query):
        """
        Find documents in a collection that match the query.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list):
                return "ERR Collection does not exist"

            try:
                query = json.loads(query)
            except json.JSONDecodeError:
                return "ERR Invalid JSON query"

            result = [doc for doc in store[key] if self.__matches_query(doc, query)]
            logger.info(f"FIND {key} -> {result}")
            return result

    def update(self, store, key, query, update_fields):
        """
        Update documents in a collection based on a query.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], list):
                return "ERR Collection does not exist"

            try:
                query = json.loads(query)
                update_fields = json.loads(update_fields)
            except json.JSONDecodeError:
                return "ERR Invalid JSON"

            updated_count = 0
            for doc in store[key]:
                if self.__matches_query(doc, query):
                    doc.update(update_fields)
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

            try:
                query = json.loads(query)
            except json.JSONDecodeError:
                return "ERR Invalid JSON query"

            initial_count = len(store[key])
            store[key] = [doc for doc in store[key] if not self.__matches_query(doc, query)]
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

    def __matches_query(self, document, query):
        """
        Helper method to match a document against a query.
        """
        for key, value in query.items():
            if key not in document or document[key] != value:
                return False
        return True
