import hashlib
from .lua_engine import LuaEngine

class ScriptManager:
    def __init__(self):
        self.scripts = {}
        self.lua_engine = LuaEngine()

    def load_script(self, script_content):
        sha_hash = hashlib.sha1(script_content.encode('utf-8')).hexdigest()
        if sha_hash not in self.scripts:
            self.scripts[sha_hash] = script_content
        return sha_hash

    def run_script(self, sha_hash, data_store, keys=[], args=[]):
        script = self.scripts.get(sha_hash, None)
        if not script:
            return "NOSCRIPT: Script not found"
        return self.lua_engine.evaluate(script, data_store, keys, args)
