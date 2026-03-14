from __future__ import annotations

import hashlib
import hmac
import os


class PasswordHasher:
    def hash_password(self, password: str) -> str:
        salt = os.urandom(16)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120_000)
        return f"pbkdf2_sha256$120000${salt.hex()}${digest.hex()}"

    def verify_password(self, password: str, encoded: str) -> bool:
        try:
            algorithm, iterations_text, salt_hex, digest_hex = encoded.split("$", 3)
        except ValueError:
            return False
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(iterations_text)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
        actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
        return hmac.compare_digest(actual, expected)
