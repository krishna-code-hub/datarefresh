from ff3 import FF3Cipher


key = "2DE79D232DF5585D68CE47882AE256D6"
tweak = "CBD09280979564"
c = FF3Cipher(key, tweak)

plaintext = "3992520240"
ciphertext = c.encrypt(plaintext)
decrypted = c.decrypt(ciphertext)

print(f"{plaintext} -> {ciphertext} -> {decrypted}")

c6 = FF3Cipher.withCustomAlphabet(key, tweak, "ABCDEF")
plaintext = "BADDCAFErty"
ciphertext = c6.encrypt(plaintext)
decrypted = c6.decrypt(ciphertext)

print(f"{plaintext} -> {ciphertext} -> {decrypted}")

# format encrypted value
ccn = f"{ciphertext[:4]} {ciphertext[4:8]} {ciphertext[8:12]} {ciphertext[12:]}"
print(f"Encrypted CCN value with formatting: {ccn}")
