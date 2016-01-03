import hashlib

# Open,close, read file and calculate MD5 on its contents
with open(r"C:\Users\Jean\Downloads\Python\python-2.7.11.amd64.msi", "rb") as file_to_check:
    # read contents of the file
    data = file_to_check.read()
    # pipe contents of the file through
    file_sha256 = hashlib.sha256(data).hexdigest()
    print("SHA-256..: " + file_sha256)
    file_md5 = hashlib.md5(data).hexdigest()
    print("MD5......: " + file_md5)