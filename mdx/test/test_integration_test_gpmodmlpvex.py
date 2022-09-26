from unittest.mock import patch
import json
import sys
from os import path
from main import MDX

sys.path.insert(0, path.join(path.dirname(__file__), '..'))

granule_name = "lpvex_SHP_Aranda_ODM_u100915_00.txt"
@patch('cumulus_process.Process.fetch_all',
       return_value={'input_key': [path.join(path.dirname(__file__), f"fixtures/{granule_name}")]})
@patch('main.MDX.upload_output_files',
       return_value=['s3://foo/lpvex_SHP_Aranda_ODM_u100915_00.txt',
                     's3://foo/lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.json'])
@patch('os.remove', return_value=granule_name)
@patch('os.path.getsize', return_value=2225)
def test_task(mock_fetch, mock_upload,mock_remove, mock_size):
    payload = path.join(path.dirname(__file__), 'gpmodmlpvex.json')
    with open(payload) as f:
        event = json.loads(f.read())
    process = MDX(input=event.get('input'), config=event.get('config'))
    x = process.process()
    expected_result = {'granules': [{'granuleId': 'lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                     'files': [{'bucket': 'foo', 'fileName': 'lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                                'key': 'test/fixtures/lpvex_SHP_Aranda_ODM_u100915_00.txt'},
                                               {'bucket': 'foo', 'fileName': 'lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                                'key': 'lpvex_SHP_Aranda_ODM_u100915_00.txt', 'size': 2225},
                                               {'bucket': 'foo',
                                                'fileName': 'lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.json',
                                                'key': 'lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.json', 'size': 1983}]}],
                       'input': ['s3://foo/lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                 's3://foo/lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.json'],
                       'system_bucket': 'foo'}
    print(x)
    print(expected_result)
    assert (x == expected_result)
