from FPE import FPE

if __name__ == "__main__":
    T = FPE.generate_tweak(8)

    key = FPE.generate_key()

    print(key)

    cipher = FPE.New(key, T, FPE.Mode.FF1)

    ciphertext = cipher.encrypt("123456", FPE.Format.DIGITS)

    print(ciphertext)

    plaintext = cipher.decrypt(ciphertext, FPE.Format.DIGITS)

    print(plaintext)
