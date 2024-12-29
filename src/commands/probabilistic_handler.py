from .base_handler import BaseCommandHandler

class ProbabilisticCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "PFADD": self.pfadd_command,
            "PFCOUNT": self.pfcount_command,
            "PFMERGE": self.pfmerge_command,
        }

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
