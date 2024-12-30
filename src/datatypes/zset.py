from typing import List, Optional, Tuple
import random

class SkipListNode:
    def __init__(self, score: float, member: str, level: int):
        self.score = score
        self.member = member
        self.forward = [None] * (level + 1)
        self.span = [0] * (level + 1)  # Add span for rank calculations

class SkipList:
    """
    A SkipList is a probabilistic data structure that allows for fast search, insertion,
    and deletion operations.
    It consists of multiple levels of linked lists, where each level is a subset of the level
    below it.
    The bottom level contains all the elements, and higher levels provide shortcuts 
    to improve efficiency.
    Attributes:
        MAX_LEVEL (int): The maximum level of the skip list.
        P (float): The probability of promoting an element to the next level.
        head (SkipListNode): The head node of the skip list.
        level (int): The current maximum level of the skip list.
        length (int): The number of elements in the skip list.
    """
    MAX_LEVEL = 16
    P = 0.25

    def __init__(self):
        self.head = SkipListNode(float('-inf'), '', self.MAX_LEVEL)
        self.level = 0
        self.length = 0

    def random_level(self) -> int:
        "Generates a random level for a new node based on the probability P."
        level = 0
        while random.random() < self.P and level < self.MAX_LEVEL:
            level += 1
        return level

    def insert(self, score: float, member: str) -> None:
        "Inserts a new element with the given score and member into the skip list."
        update = [None] * (self.MAX_LEVEL + 1)
        rank = [0] * (self.MAX_LEVEL + 1)
        current = self.head

        # Find position and calculate rank
        for i in range(self.level, -1, -1):
            rank[i] = rank[i + 1] if i < self.level else 0
            while (current.forward[i] and 
                  (current.forward[i].score < score or 
                   (current.forward[i].score == score and 
                    current.forward[i].member < member))):
                rank[i] += current.span[i]
                current = current.forward[i]
            update[i] = current

        level = self.random_level()
        if level > self.level:
            for i in range(self.level + 1, level + 1):
                rank[i] = 0
                update[i] = self.head
                update[i].span[i] = self.length
            self.level = level

        # Create new node
        new_node = SkipListNode(score, member, level)
        
        # Insert node and update spans
        for i in range(level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node
            
            # Update spans
            new_node.span[i] = update[i].span[i] - (rank[0] - rank[i])
            update[i].span[i] = (rank[0] - rank[i]) + 1

        # Update spans for untouched levels
        for i in range(level + 1, self.level + 1):
            update[i].span[i] += 1

        self.length += 1

    def delete(self, score: float, member: str) -> bool:
        update = [None] * (self.MAX_LEVEL + 1)
        current = self.head

        # Find node to delete
        for i in range(self.level, -1, -1):
            while (current.forward[i] and 
                  (current.forward[i].score < score or 
                   (current.forward[i].score == score and 
                    current.forward[i].member < member))):
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]
        if current and current.score == score and current.member == member:
            # Remove node at all levels
            for i in range(self.level + 1):
                if update[i].forward[i] != current:
                    break
                update[i].forward[i] = current.forward[i]

            # Update level if needed
            while self.level > 0 and not self.head.forward[self.level]:
                self.level -= 1

            self.length -= 1
            return True
        return False

    def get_rank(self, member: str, score: float) -> Optional[int]:
        rank = 0
        current = self.head

        # Traverse all levels from top to bottom
        for i in range(self.level, -1, -1):
            while (current.forward[i] and 
                  (current.forward[i].score < score or 
                   (current.forward[i].score == score and 
                    current.forward[i].member <= member))):
                rank += current.span[i]
                current = current.forward[i]
                if (current.score == score and 
                    current.member == member):
                    return rank - 1  # Adjust for 0-based ranking

        return None

    def get_range(self, start: int, stop: int) -> List[Tuple[str, float]]:
        if start < 0:
            start = max(self.length + start, 0)
        if stop < 0:
            stop = self.length + stop
        
        stop = min(stop, self.length - 1)
        
        if start > stop or start >= self.length:
            return []

        result = []
        rank = 0
        current = self.head

        # Skip to start position using spans
        for i in range(self.level, -1, -1):
            while current.forward[i] and (rank + current.span[i]) <= start:
                rank += current.span[i]
                current = current.forward[i]

        current = current.forward[0]
        while current and len(result) <= (stop - start):
            result.append((current.member, current.score))
            current = current.forward[0]

        return result

class ZSetDataType:
    def __init__(self, database):
        self.db = database

    def _ensure_zset(self, key):
        """Ensure the value at key is a sorted set."""
        if not self.db.exists(key):
            value = {'dict': {}, 'skiplist': SkipList()}
            self.db.store[key] = value
            return value
        value = self.db.store.get(key)
        if not isinstance(value, dict) or 'dict' not in value or 'skiplist' not in value:
            raise ValueError("WRONGTYPE Operation against a key holding the wrong kind of value")
        return value

    def zadd(self, key, *args):
        """Add members to sorted set."""
        if len(args) % 2 != 0:
            raise ValueError("ERR syntax error")
        
        try:
            zset = self._ensure_zset(key)
            added = 0
            
            # Process score-member pairs
            i = 0
            while i < len(args):
                score = float(args[i])
                member = str(args[i + 1])
                
                if member not in zset['dict']:
                    added += 1
                elif zset['dict'][member] == score:
                    i += 2
                    continue
                    
                # Update or add member
                if member in zset['dict']:
                    zset['skiplist'].delete(zset['dict'][member], member)
                zset['dict'][member] = score
                zset['skiplist'].insert(score, member)
                
                i += 2
            
            if added > 0 and not self.db.replaying:
                self.db.persistence_manager.log_command(f"ZADD {key} {' '.join(map(str, args))}")
            
            return added
        except ValueError as e:
            raise ValueError(f"ERR {str(e)}")

    def zrange(self, key, start, stop, withscores=False):
        """Return range of members by index."""
        try:
            zset = self._ensure_zset(key)
            result = zset['skiplist'].get_range(start, stop)
            if not result:
                return []
            if withscores:
                return [(item[0], str(item[1])) for item in result]
            return [item[0] for item in result]
        except ValueError as e:
            return str(e)

    def zrank(self, key: str, member: str) -> Optional[int]:
        """Return rank of member in sorted set."""
        try:
            zset = self._ensure_zset(key)
            if member not in zset['dict']:
                return None
            return zset['skiplist'].get_rank(member, zset['dict'][member])
        except ValueError:
            return None

    def zrem(self, key: str, *members: str) -> int:
        """Remove members from sorted set."""
        try:
            zset = self._ensure_zset(key)
            removed = 0
            for member in members:
                if member in zset['dict']:
                    score = zset['dict'][member]
                    if zset['skiplist'].delete(score, member):
                        del zset['dict'][member]
                        removed += 1
            
            if removed and not self.db.replaying:
                members_str = ' '.join(members)
                self.db.persistence_manager.log_command(f"ZREM {key} {members_str}")
            
            return removed
        except ValueError:
            return 0

    def zrangebyscore(self, key: str, min_score: str, max_score: str, withscores: bool = False) -> List:
        """Return members with scores within the given range."""
        try:
            zset = self._ensure_zset(key)
            try:
                min_val = float('-inf') if min_score == '-inf' else float(min_score)
                max_val = float('inf') if max_score == '+inf' else float(max_score)
            except ValueError:
                return []

            result = []
            current = zset['skiplist'].head.forward[0]
            while current and current.score <= max_val:
                if current.score >= min_val:
                    if withscores:
                        result.append((current.member, self._format_score(current.score)))
                    else:
                        result.append(current.member)
                current = current.forward[0]
            return result
        except ValueError:
            return []
