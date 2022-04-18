import os
import json

def credential() :
    script_dir = os.path.dirname(__file__)
    dir_file_c = os.path.join(script_dir, '../credential.json')
    with open (dir_file_c, "r") as cred:
        credential =  json.load(cred)
    return credential