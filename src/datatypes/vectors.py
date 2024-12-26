# src/datatypes/vectors.py
from src.logger import setup_logger
import threading
import math

logger = setup_logger("vectors")

class Vectors:
    def __init__(self):
        self.lock = threading.Lock()

    def _validate_vector(self, vector):
        if not isinstance(vector, list) or not all(isinstance(v, (int, float)) for v in vector):
            return "ERR Vector must be a list of numbers"
        return None

    def add_vector(self, store, key, vector):
        """
        Add or update a vector in the store.
        """
        with self.lock:
            error = self._validate_vector(vector)
            if error:
                return error
            if key in store and not isinstance(store[key], list):
                return "ERR Key exists and is not a vector"
            store[key] = vector
            logger.info(f"VECTOR.ADD {key} -> {vector}")
            return "OK"

    def similarity_search(self, store, query_vector, metric="cosine", top_k=1):
        """
        Perform a similarity search and return the top-k nearest vectors.
        """
        with self.lock:
            error = self._validate_vector(query_vector)
            if error:
                return error

            distances = []
            for key, vector in store.items():
                if not isinstance(vector, list):
                    continue

                if metric == "cosine":
                    dist = self.__cosine_similarity(query_vector, vector)
                elif metric == "euclidean":
                    dist = self.__euclidean_distance(query_vector, vector)
                else:
                    return "ERR Unknown metric"

                distances.append((key, dist))

            reverse_sort = (metric == "cosine")
            distances.sort(key=lambda x: x[1], reverse=reverse_sort)
            result = distances[:top_k]
            logger.info(f"SIMILARITY SEARCH -> {result}")
            return result

    def vector_operation(self, store, op, vector1, vector2):
        """
        Perform arithmetic operations on two vectors.
        """
        error1 = self._validate_vector(vector1)
        error2 = self._validate_vector(vector2)
        if error1:
            return error1
        if error2:
            return error2

        if len(vector1) != len(vector2):
            return "ERR Vectors must have the same length"

        if op == "add":
            result = [a + b for a, b in zip(vector1, vector2)]
        elif op == "sub":
            result = [a - b for a, b in zip(vector1, vector2)]
        elif op == "dot":
            result = sum(a * b for a, b in zip(vector1, vector2))
        else:
            return "ERR Unknown operation"

        logger.info(f"VECTOR.{op.upper()} -> {result}")
        return result

    def __cosine_similarity(self, vec1, vec2):
        """
        Calculate the cosine similarity between two vectors.
        """
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = math.sqrt(sum(a ** 2 for a in vec1))
        magnitude2 = math.sqrt(sum(a ** 2 for a in vec2))
        return dot_product / (magnitude1 * magnitude2) if magnitude1 and magnitude2 else 0

    def __euclidean_distance(self, vec1, vec2):
        """
        Calculate the Euclidean distance between two vectors.
        """
        return math.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2)))
