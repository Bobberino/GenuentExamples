from cryptography.fernet import Fernet

key = b'rI1J3H2hVsu9amHcr6wC9PXGbBmS-dYtPv0A4goOFBw='
cipher_suite = Fernet(key)
ciphered_text = cipher_suite.encrypt(b"SuperSecretPassword")



with open('mssqltip_bytes.bin', 'wb') as file_object:
    file_object.write(ciphered_text)

with open('c:\savedfiles\mssqltip_bytes.bin', 'rb') as file_object:
    for line in file_object:
        encryptedpwd = line
print(encryptedpwd)

uncipher_text = (cipher_suite.decrypt(encryptedpwd))

plain_text_encryptedpassword = bytes(uncipher_text).decode("utf-8") #convert to string
print(plain_text_encryptedpassword)
