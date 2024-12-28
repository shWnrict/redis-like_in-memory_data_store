# src/datatypes/vectors.py
from src.datatypes.base import BaseDataType  # Import BaseDataType
from src.logger import setup_logger
import threading
import math
import numpy as np
from typing import List, Tuple

logger = setup_logger("vectors")

class Vectors(BaseDataType):
    def __init__(self, store, expiry_manager=None):
        super().__init__(store, expiry_manager)
        self.lock = threading.Lock()
        self.dimension = None

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

    def vector_set(self, store, key, *values):
        """
        Set a vector with the given values.
        """
        with self.lock:
            vector = [float(v) for v in values]
            store[key] = vector
            logger.info(f"VECTOR.SET {key} -> {vector}")
            return "OK"

    def vector_add(self, store, key1, key2, destination_key):
        """
        Add two vectors and store the result in destination_key.
        """
        with self.lock:
            vec1 = store.get(key1)
            vec2 = store.get(key2)
            if not isinstance(vec1, list) or not isinstance(vec2, list):
                return "ERR One of the keys does not contain a vector"
            if len(vec1) != len(vec2):
                return "ERR Vectors must be of the same length"

            result = [a + b for a, b in zip(vec1, vec2)]
            store[destination_key] = result
            logger.info(f"VECTOR.ADD {key1} + {key2} -> {destination_key}: {result}")
            return "OK"

    def vector_dot(self, store, key1, key2):
        """
        Calculate the dot product of two vectors.
        """
        with self.lock:
            vec1 = store.get(key1)
            vec2 = store.get(key2)
            if not isinstance(vec1, list) or not isinstance(vec2, list):
                return "ERR One of the keys does not contain a vector"
            if len(vec1) != len(vec2):
                return "ERR Vectors must be of the same length"
            
            dot_product = sum(a * b for a, b in zip(vec1, vec2))
            logger.info(f"VECTOR.DOT {key1} . {key2} -> {dot_product}")
            return dot_product

    def vector_similarity(self, store, key1, key2):
        """
        Calculate the cosine similarity between two vectors.
        """
        with self.lock:
            vec1 = store.get(key1)
            vec2 = store.get(key2)
            if not isinstance(vec1, list) or not isinstance(vec2, list):
                return "ERR One of the keys does not contain a vector"
            if len(vec1) != len(vec2):
                return "ERR Vectors must be of the same length"

            dot_product = sum(a * b for a, b in zip(vec1, vec2))
            magnitude1 = math.sqrt(sum(a ** 2 for a in vec1))
            magnitude2 = math.sqrt(sum(b ** 2 for b in vec2))
            if magnitude1 == 0 or magnitude2 == 0:
                return "ERR Zero magnitude vector"
            similarity = dot_product / (magnitude1 * magnitude2)
            logger.info(f"VECTOR.SIMILARITY {key1} ~ {key2} -> {similarity}")
            return similarity

    def vector_add(self, store, key, vector):
        """Store a vector"""
        with self.lock:
            try:
                vec = np.array(vector, dtype=np.float32)
                if self.dimension is None:
                    self.dimension = vec.shape[0]
                elif vec.shape[0] != self.dimension:
                    return "ERR Vector dimension mismatch"
                store[key] = vec
                return "OK"
            except Exception as e:
                return f"ERR {str(e)}"

    def cosine_similarity(self, v1, v2) -> float:
        """Calculate cosine similarity between two vectors"""
        dot_product = np.dot(v1, v2)
        norm_v1 = np.linalg.norm(v1)
        norm_v2 = np.linalg.norm(v2)
        return dot_product / (norm_v1 * norm_v2)

    def knn_search(self, store, query_vector, k: int) -> List[Tuple[str, float]]:
        """Find k-nearest neighbors"""
        with self.lock:
            try:
                query_vec = np.array(query_vector, dtype=np.float32)
                results = []
                for key, vec in store.items():
                    if isinstance(vec, np.ndarray):
                        similarity = self.cosine_similarity(query_vec, vec)
                        results.append((key, similarity))
                results.sort(key=lambda x: x[1], reverse=True)
                return results[:k]
            except Exception as e:
                logger.error(f"KNN search error: {e}")
                return []

    def handle_command(self, cmd, store, *args):
        cmd = cmd.upper()
        if cmd == "VECTOR.SET":
            return self.vector_set(store, args[0], *args[1:])
        elif cmd == "VECTOR.ADD":
            if len(args) < 2:
                return "-ERR wrong number of arguments\r\n"
            key = args[0]
            try:
                vector = [float(x) for x in args[1:]]
                result = self.vector_add(store, key, vector)
                return f"+{result}\r\n"
            except ValueError:
                return "-ERR invalid vector values\r\n"
        elif cmd == "VECTOR.KNN":
            if len(args) < 2:
                return "-ERR wrong number of arguments\r\n"
            try:
                k = int(args[0])
                vector = [float(x) for x in args[1:]]
                results = self.knn_search(store, vector, k)
                return self._format_knn_results(results)
            except ValueError:
                return "-ERR invalid arguments\r\n"
        elif cmd == "VECTOR.DOT":
            if len(args) != 2:
                return "ERR VECTOR.DOT requires 2 arguments"
            return self.vector_dot(store, args[0], args[1])
        elif cmd == "VECTOR.SIMILARITY":
            if len(args) != 2:
                return "ERR VECTOR.SIMILARITY requires 2 arguments"
            return self.vector_similarity(store, args[0], args[1])
        return "ERR Unknown command"
