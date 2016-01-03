import hashlib

# Open,close, read file and calculate MD5 on its contents
with open(r"C:\Users\Jean\Downloads\VirtualBox\VirtualBox-5.0.12-104815-Win.exe", "rb") as file_to_check:
    # read contents of the file
    data = file_to_check.read()
    # pipe contents of the file through
    file_sha256 = hashlib.sha256(data).hexdigest()
    print(file_sha256)