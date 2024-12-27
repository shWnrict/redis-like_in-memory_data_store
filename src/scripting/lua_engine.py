# src/scripting/lua_engine.py
import lupa
from lupa import LuaRuntime
from src.logger import setup_logger
from typing import Any, List

logger = setup_logger("lua_engine")

class LuaEngine:
    def __init__(self, server):
        self.server = server
        self.lua = LuaRuntime(unpack_returned_tuples=True)
        logger.info("LuaEngine initialized.")

    def execute_script(self, script: str, keys: List[str], args: List[Any]) -> Any:
        """
        Execute a Lua script with the given keys and arguments.
        """
        try:
            lua_func = self.lua.eval(f"function(...)\n{script}\nend")
            result = lua_func(*keys, *args)
            logger.info("Lua script executed successfully.")
            return result
        except lupa.LuaError as e:
            logger.error(f"Lua script execution error: {e}")
            return f"ERR {str(e)}"
