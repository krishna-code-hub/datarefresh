import logging
import os
from masking.fpe_masking import FPEMasking
from masking.ff3_masking import FF3Masking
from masking.abstract_masking import BaseMasking


class MaskingFactory:
    ALPHABETS = {
        "DIGITS": "0123456789",
        "LETTERS": "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ",
        "STRING": "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
        "EMAIL": "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789@._",
    }

    @staticmethod
    def get_masking_algorithm(algorithm_type, key, tweak, format_type="DIGITS", alphabet=None):
        # Retrieve the key from the environment
        if not key:
            raise ValueError(f"Encryption Key is not set")

        # Ensure key is the right length for each algorithm
        if algorithm_type == "fpe_ff1":
            if len(key) != 32:  # 128-bit key required
                raise ValueError("Key must be 128 bits (32 hex characters) for FPE FF1")
            return FPEMasking(key, tweak, mode="FF1")
        elif algorithm_type == "ff3":
            alphabet = MaskingFactory.ALPHABETS.get(alphabet, MaskingFactory.ALPHABETS["DIGITS"])
            if len(key) not in [32, 64]:  # 128-bit or 256-bit key required
                raise ValueError("Key must be 128 or 256 bits for FF3")
            if alphabet is None:
                alphabet = "0123456789"  # Default to digits if no alphabet specified
            logging.debug(f"passing alphabet {alphabet} for masking")
            return FF3Masking(key, tweak, alphabet=alphabet)
        else:
            raise ValueError(f"Unsupported masking algorithm: {algorithm_type}")
