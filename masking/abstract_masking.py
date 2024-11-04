from abc import ABC, abstractmethod


class BaseMasking(ABC):
    @abstractmethod
    async def encrypt(self, plaintext):
        """Encrypt the plaintext and return the ciphertext."""
        pass

    @abstractmethod
    async def decrypt(self, ciphertext):
        """Decrypt the ciphertext and return the plaintext."""
        pass
