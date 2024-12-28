import hashlib
import os
from src.logger import setup_logger

logger = setup_logger("auth")

class Authentication:
    def __init__(self):
        self.users = {}
        self.acl_lists = {}

    def add_user(self, username, password):
        """Add a new user with hashed password"""
        salt = os.urandom(16)
        hashed = self._hash_password(password, salt)
        self.users[username] = {
            'hash': hashed,
            'salt': salt
        }
        return True

    def authenticate(self, username, password):
        """Verify user credentials"""
        if username not in self.users:
            return False
        user = self.users[username]
        hashed = self._hash_password(password, user['salt'])
        return hashed == user['hash']

    def _hash_password(self, password, salt):
        """Hash password with salt using SHA-256"""
        return hashlib.pbkdf2_hmac(
            'sha256',
            password.encode(),
            salt,
            100000
        )

    def set_acl(self, username, commands):
        """Set allowed commands for user"""
        self.acl_lists[username] = set(commands)

    def check_permission(self, username, command):
        """Check if user has permission for command"""
        if username not in self.acl_lists:
            return False
        return command in self.acl_lists[username]
