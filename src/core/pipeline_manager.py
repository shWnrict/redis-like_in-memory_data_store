# src/core/pipeline_manager.py
from typing import List, Any
from threading import Lock
import logging
from src.logger import setup_logger
from src.protocol import RESPProtocol  # Import RESPProtocol

logger = setup_logger("pipeline_manager")

class PipelineManager:
    """
    Manages batching of commands for pipeline execution.
    """

    def __init__(self, command_router):
        self.command_router = command_router
        self.pipeline_commands: List[List[str]] = []
        self.lock = Lock()
        self.max_pipeline_length = 10000  # Safety limit

    def start_pipeline(self):
        """Start a new pipeline"""
        with self.lock:
            self.pipeline_commands = []
            return "+OK\r\n"

    def add_command(self, command: List[str]) -> str:
        """Add command to pipeline"""
        with self.lock:
            if len(self.pipeline_commands) >= self.max_pipeline_length:
                return "-ERR Pipeline too long\r\n"
            self.pipeline_commands.append(command)
            return "+QUEUED\r\n"

    def execute_pipeline(self, client_socket=None) -> List[str]:
        """Execute all commands in pipeline"""
        with self.lock:
            responses = []
            for cmd in self.pipeline_commands:
                try:
                    response = self.command_router.route(cmd, client_socket)
                    responses.append(RESPProtocol.format_response(response))
                except Exception as e:
                    responses.append(f"-ERR {str(e)}\r\n")
            self.pipeline_commands = []
            return responses
