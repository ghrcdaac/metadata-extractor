import json
import os
import sys

import requests

# if __name__ == '__main__':
#     # Create the release
#     url = "https://gitlab.com/api/v4/projects/19420926/releases"
#     headers = {'PRIVATE-TOKEN': os.environ['CI_JOB_TOKEN']}
#     data = {
#         'tag_name': 'mdx_lite',
#         'assets': {
#             'links': [
#                 {
#                     'name': 'link_test',
#                     'url': 'https://gitlab.com/api/v4/projects/19420926/mdx_lite/',
#                     'filepath': 'package.zip'
#                 }
#             ]
#         }
#     }
#     post_resp = requests.post(url, headers=headers, data=data)
#     print(post_resp.text)

# if __name__ == '__main__':
#     files = {'file': open('package.zip', 'rb')}
#     print(sys.getsizeof(files))
#     url = "https://gitlab.com/api/v4/projects/19420926/uploads"
#     headers = {'PRIVATE-TOKEN': os.environ['CI_JOB_TOKEN']}
#     post_resp = requests.post(url, headers=headers, files=files)
#     print(post_resp.text)

# Create file of x bytes
# if __name__ == '__main__':
#     f = open('testfile', 'wb')
#     x = 10000000
#     while x > 0:
#         f.write(b'0')
#         x -= 1

# if __name__ == '__main__':
#     url = "https://gitlab.com/api/v4/projects/19420926/releases/test_tag"
#     headers = {'PRIVATE-TOKEN': os.environ['CI_JOB_TOKEN']}
#     data = {'tag_name': 'Test'}
#     data = {}
#     post_resp = requests.delete(url, headers=headers, data=data)
#     print(post_resp.text)


# Create release
# if __name__ == '__main__':
#     url = "https://gitlab.com/api/v4/projects/19420926/releases"
#     headers = {'PRIVATE-TOKEN': os.environ['CI_JOB_TOKEN']}
#     data = {'tag_name': 'mdx_lite', 'ref': 'HEAD'}
#     post_resp = requests.post(url, headers=headers, data=data)
#     print(post_resp.text)

# Get artifacts for last CreateReleasePackageLite stage in jobs
if __name__ == "__main__":
    # url = f"https://gitlab.com/api/v4/projects/19420926/pipelines/{os.environ['CI_PIPELINE_ID']}/jobs"
    url = f"https://gitlab.com/api/v4/projects/19420926/jobs"
    # headers = {'PRIVATE-TOKEN': os.environ['CI_BUILD_TOKEN']}
    # headers = {'PRIVATE-TOKEN': os.environ['CI_JOB_TOKEN']}
    headers = {'JOB-TOKEN': os.environ['CI_JOB_TOKEN']}
    data = {'scope': ['success']}
    post_resp = requests.get(url, headers=headers)
    # Get the last successful job id (add logic to not call this if the checks for merging into master fail)
    job_id = ''
    response_json = post_resp.json()
    print(response_json)
    if type(response_json) is list:
        for entry in response_json:
            if entry['stage'] == 'CreateReleasePackageLite':
                job_id = entry['id']
                print(type(entry))
    else:
        if response_json['stage'] == 'CreateReleasePackageLite':
            job_id = response_json['id']


    artifact_link = f"https://gitlab.com/ghrc-cloud/metadata-extractor/-/jobs/{job_id}/artifacts/download"
    # print(artifact_link)

    # Create the release
    url = "https://gitlab.com/api/v4/projects/19420926/releases"
    headers = {'PRIVATE-TOKEN': os.environ['CI_JOB_TOKEN']}
    data = {
        'tag_name': 'mdx_lite',
        'assets': {
            'links': [
                {
                    'name': 'mdx_lite',
                    'url': artifact_link
                }
            ]
        }
    }
    post_resp = requests.post(url, headers=headers, data=data)
    # print(post_resp.text)


