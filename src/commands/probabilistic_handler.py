from .base_handler import BaseCommandHandler

class ProbabilisticCommandHandler(BaseCommandHandler):
    def get_commands(self):
        commands = {
            "BF.RESERVE": self.bf_reserve_command,
            "BF.ADD": self.bf_add_command,
            "BF.EXISTS": self.bf_exists_command,
        }
        commands.update({
            "PFADD": self.pfadd_command,
            "PFCOUNT": self.pfcount_command,
            "PFMERGE": self.pfmerge_command,
        })
        return commands

    def bf_reserve_command(self, client_id, key, size, num_hashes):
        """Create a new Bloom filter. Format: BF.RESERVE key size num_hashes"""
        try:
            size = int(size)
            num_hashes = int(num_hashes)
            if size <= 0 or num_hashes <= 0:
                return "ERROR: Size and num_hashes must be positive"
            return "OK" if self.db.probabilistic.bf_reserve(key, size, num_hashes) else "ERROR: Key exists"
        except ValueError:
            return "ERROR: Invalid arguments"

    def bf_add_command(self, client_id, key, item):
        """Add item to Bloom filter. Format: BF.ADD key item"""
        if not item:
            return "ERROR: Wrong number of arguments for BF.ADD"
        return str(self.db.probabilistic.bf_add(key, item))

    def bf_exists_command(self, client_id, key, item):
        """Check if item might exist. Format: BF.EXISTS key item"""
        if not item:
            return "ERROR: Wrong number of arguments for BF.EXISTS"
        return "1" if self.db.probabilistic.bf_exists(key, item) else "0"

    def pfadd_command(self, client_id, key, *elements):
        """Add elements to HLL. Format: PFADD key element [element ...]"""
        if not elements:
            return "ERROR: Wrong number of arguments for PFADD"
        return str(self.db.probabilistic.pfadd(key, *elements))

    def pfcount_command(self, client_id, *keys):
        """Get cardinality estimate. Format: PFCOUNT key [key ...]"""
        if not keys:
            return "ERROR: Wrong number of arguments for PFCOUNT"
        return str(self.db.probabilistic.pfcount(*keys))

    def pfmerge_command(self, client_id, destkey, *sourcekeys):
        """Merge HLLs. Format: PFMERGE destkey sourcekey [sourcekey ...]"""
        if not sourcekeys:
            return "ERROR: Wrong number of arguments for PFMERGE"
        return "OK" if self.db.probabilistic.pfmerge(destkey, *sourcekeys) else "ERROR"
