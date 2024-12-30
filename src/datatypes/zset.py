from typing import List, Optional, Tuple
import random

class SkipListNode:
    def __init__(self, score: float, member: str, level: int):
        self.score = score
        self.member = member
        self.forward = [None] * (level + 1)
        self.span = [0] * (level + 1)  # Add span for rank calculations

class SkipList:
    MAX_LEVEL = 16
    P = 0.25

    def __init__(self):
        self.head = SkipListNode(float('-inf'), '', self.MAX_LEVEL)
        self.level = 0
        self.length = 0

    def random_level(self) -> int:
        level = 0
        while random.random() < self.P and level < self.MAX_LEVEL:
            level += 1
        return level

    def insert(self, score: float, member: str) -> None:
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
        value = self.db.get(key)
        if not isinstance(value, dict) or 'dict' not in value or 'skiplist' not in value:
            raise ValueError("Value is not a sorted set")
        return value

    def _format_score(self, score: float) -> str:
        """Format score to remove unnecessary decimal places."""
        if score.is_integer():
            return str(int(score))
        return str(score)

    def zadd(self, key: str, *args) -> int:
        """Add members to sorted set."""
        if len(args) % 2 != 0:
            raise ValueError("ZADD requires score-member pairs")
        
        try:
            zset = self._ensure_zset(key)
            added = 0
            pairs = list(zip(args[::2], args[1::2]))
            
            for score_str, member in pairs:
                try:
                    score = float(score_str)
                except ValueError:
                    continue
                
                if member not in zset['dict']:
                    added += 1
                
                # Always update the skiplist
                if member in zset['dict']:
                    zset['skiplist'].delete(zset['dict'][member], member)
                zset['dict'][member] = score
                zset['skiplist'].insert(score, member)
            
            if added and not self.db.replaying:
                args_str = ' '.join(str(x) for x in args)
                self.db.persistence_manager.log_command(f"ZADD {key} {args_str}")
            
            return added
        except ValueError:
            return 0

    def zrange(self, key: str, start: int, stop: int, withscores: bool = False) -> List:
        """Return range of members by index."""
        try:
            zset = self._ensure_zset(key)
            range_result = zset['skiplist'].get_range(int(start), int(stop))
            if withscores:
                return [(member, self._format_score(score)) for member, score in range_result]
            return [member for member, _ in range_result]
        except ValueError:
            return []

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
