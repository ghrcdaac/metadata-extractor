import os  # -- Needed for locating python metadata codebase
from datetime import datetime, timedelta
import json
import tarfile
from hashlib import md5


def read_tcspecmwf_metadata(file_path):
    metadata = {}
    north = 90.0
    south = -90.0
    east = 180.0
    west = -180.0

    file_name_list = tarfile.open(file_path).getnames()
    tarfile_name = os.path.basename(file_path)

    dt = []
    for file in file_name_list:
        tkn = file.split('.')
        dt_str = datetime.strptime(''.join(tkn[1:5]), '%Y%m%dT%HZ')
        dt.append(dt_str)

    metadata[tarfile_name] = {}
    with open(file_path, 'rb') as file:
        metadata[tarfile_name]['checksum'] = md5(file.read()).hexdigest()

    metadata[tarfile_name]['SizeMBDataGranule'] = str(round(1E-6 * os.path.getsize(file_path), 2))

    metadata[tarfile_name]['temporal'] = [min(dt).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                          max(dt).strftime('%Y-%m-%dT%H:%M:%SZ')]
    metadata[tarfile_name]['wnes_geometry'] = list(str(x) for x in [west, north, east, south])
    return metadata


file_dir = '/c/Users/ecampos/Downloads/tcspecmwf/'
full_md = {}
for subdir, dirs, files in os.walk(file_dir):
    for filename in files:
        file_md = read_tcspecmwf_metadata(f"{file_dir}{filename}")
        full_md.update(file_md)

#print(full_md)
with open('../tscpecmwfRefData.json', 'w') as fp:
    json.dump(full_md, fp)
