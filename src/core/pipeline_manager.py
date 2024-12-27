# src/core/pipeline_manager.py
from typing import List, Any
from threading import Lock
import logging

logger = logging.getLogger(__name__)

class PipelineManager:
    """
    Manages batching of commands for pipeline execution.
    """

    def __init__(self, command_router):
        self.command_router = command_router
        self.pipeline_commands: List[List[str]] = []
        self.lock = Lock()

    def add_command(self, command: List[str]):
        """
        Add a command to the pipeline.
        """
        with self.lock:
            self.pipeline_commands.append(command)
            logger.debug(f"Added command to pipeline: {command}")
            return "QUEUED"

    def execute(self, client_socket=None):
        """
        Execute all commands in the pipeline atomically.
        """
        with self.lock:
            responses = []
            for command in self.pipeline_commands:
                response = self.command_router.route(command, client_socket)
                responses.append(response)
            self.pipeline_commands.clear()
            logger.info("Executed pipeline commands.")
            return responses
