with open('secret_key.txt', 'r') as f:
    SECRET_KEY = f.read().strip()

print(SECRET_KEY)