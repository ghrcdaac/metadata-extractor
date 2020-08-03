from unittest.mock import patch
import json
import sys
from os import path

sys.path.insert(0, path.join(path.dirname(__file__), '..'))
# from handler import task
from process_mdx.main import MDX
@patch('cumulus_process.Process.fetch_all', return_value={'input_key': ["./test/fixtures/lpvex_SHP_Aranda_ODM_u100915_00.txt"]})
@patch('cumulus_process.Process.upload_output_files', return_value=['s3://lpvex_SHP_Aranda_ODM_u100915_00.txt', 's3://lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.xml'])
@patch('os.path.getsize', return_value=2225)
def test_task(mock_fetch, mock_upload, mock_size):
    payload = path.join(path.dirname(__file__), 'gpmodmlpvex.json')
    with open(payload) as f:
        event = json.loads(f.read())
    process = MDX(input=event.get('input'), config= event.get('config'))
    x = process.process()
    # x = task(event, None)
    expected_result = {'granules': [{'granuleId': 'lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                     'files': [{'path': 'gpmodmlpvex__1', 'url_path': 'gpmodmlpvex__1',
                                                'bucket': 'ghrcsbxw-protected',
                                                'fileName': 'lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                                'filename': 's3://lpvex_SHP_Aranda_ODM_u100915_00.txt', 'size': 2225},
                                               {'path': 'gpmodmlpvex__1', 'url_path': 'gpmodmlpvex__1',
                                                'bucket': 'ghrcsbxw-public',
                                                'fileName': 'lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.xml',
                                                'filename': 's3://lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.xml',
                                                'size': 2225}]}],
                       'input': ['s3://lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                 's3://lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.xml']}
    assert(x == expected_result)