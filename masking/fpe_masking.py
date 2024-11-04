from FPE import FPE
from masking.abstract_masking import BaseMasking


class FPEMasking(BaseMasking):
    def __init__(self, key, tweak, mode="FF1"):
        self.tweak = tweak
        self.cipher = FPE.New(key, tweak, FPE.Mode[mode])

    async def encrypt(self, plaintext, format_type="DIGITS"):
        return self.cipher.encrypt(plaintext, FPE.Format[format_type])

    async def decrypt(self, ciphertext, format_type="DIGITS"):
        return self.cipher.decrypt(ciphertext, FPE.Format[format_type])
