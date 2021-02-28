import hashlib

x = hashlib.md5('abc'.encode('utf-8')).hexdigest()
print(f'{x}')