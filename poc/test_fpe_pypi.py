from FPE import FPE, Format


variables = [
    "Username",
    "Password",
    "Email",
    "PhoneNumber",
    "Cpr-number",
    "Creditcard",
    "adress",
    "city",
    "zip",
    "country",
]

formats = [
    Format.LETTERS,
    Format.STRING,
    Format.EMAIL,
    Format.DIGITS,
    Format.CPR,
    Format.CREDITCARD,
    Format.STRING,
    Format.LETTERS,
    Format.DIGITS,
    Format.LETTERS,
]

if __name__ == "__main__":
    T = FPE.generate_tweak(7)
    key = FPE.generate_key()
    cipher = FPE.New(key, T, FPE.Mode.FF3)
    cipher.generateData("testData.csv", 1000, formats, variables)
    cipher.encryptCSV("testData.csv", "encryptedData.csv", formats)
    cipher.decryptCSV("encryptedData.csv", "decryptedData.csv", formats)
