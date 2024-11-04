from ff3 import FF3Cipher
from masking.abstract_masking import BaseMasking


class FF3Masking(BaseMasking):
    def __init__(self, key, tweak, alphabet="0123456789"):
        # Using withCustomAlphabet to support custom character sets in FF3
        self.cipher = FF3Cipher.withCustomAlphabet(key, tweak, alphabet)

    async def encrypt(self, plaintext):
        return self.cipher.encrypt(plaintext)

    async def decrypt(self, ciphertext):
        return self.cipher.decrypt(ciphertext)
