from unittest.mock import patch
import json
import sys
from os import path
from process_mdx.main import MDX

sys.path.insert(0, path.join(path.dirname(__file__), '..'))

granule_name = "lpvex_SHP_Aranda_ODM_u100915_00.txt"
@patch('cumulus_process.Process.fetch_all',
       return_value={'input_key': [path.join(path.dirname(__file__), f"fixtures/{granule_name}")]})
@patch('process_mdx.main.MDX.upload_output_files',
       return_value=['s3://lpvex_SHP_Aranda_ODM_u100915_00.txt',
                     's3://lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.xml'])
@patch('os.remove', return_value=granule_name)
@patch('os.path.getsize', return_value=2225)
def test_task(mock_fetch, mock_upload,mock_remove, mock_size):
    payload = path.join(path.dirname(__file__), 'gpmodmlpvex.json')
    with open(payload) as f:
        event = json.loads(f.read())
    process = MDX(input=event.get('input'), config=event.get('config'))
    x = process.process()
    expected_result = {'granules': [{'granuleId': 'lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                     'files': [
                                         {'path': 'gpmodmlpvex__1', 'url_path': 'gpmodmlpvex__1',
                                          'name': 'lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                          'filename': 's3://lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                          'size': 2225,
                                          'filepath': 'gpmodmlpvex__1/lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                          'fileStagingDir': ''},
                                         {'path': 'gpmodmlpvex__1', 'url_path': 'gpmodmlpvex__1',
                                          'name': 'lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.xml',
                                          'filename': 's3://lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.xml',
                                          'size': 0.0,
                                          'filepath': 'gpmodmlpvex__1/lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.xml',
                                          'fileStagingDir': ''}
                                     ]}],
                       'input': ['s3://lpvex_SHP_Aranda_ODM_u100915_00.txt',
                                 's3://lpvex_SHP_Aranda_ODM_u100915_00.txt.cmr.xml'],
                       'system_bucket': 'lpvex_SHP_Aranda_ODM_u100915_00.txt'}
    print(x)
    print(expected_result)
    assert (x == expected_result)
