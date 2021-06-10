import json
import os
import requests

url = "https://gitlab.com/ghrc-cloud/metadata-extractor/-/releases"
data = {"name": "New release",
        "description": "Test release",
        "assets:links:filepath": './package.zip'}
headers = {'Content-Type': 'application/json', 'PRIVATE-TOKEN': os.environ['CI_JOB_TOKEN']}
post_resp = requests.post(url, data=json.dumps(data), headers=headers)
print(post_resp)
