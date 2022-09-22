import json
import os
import re
import shutil
import sys
import tarfile
from datetime import datetime, timedelta
from hashlib import md5
import numpy
from spectral.io import envi                    # this library is only needed if you intend to
#                                                 recreate [ds_short_name]RefData.json

import pyart

metadata = {}

def get_metadata(file_path):
    #try:
    #    radar = pyart.io.read_cfradial(file_path)
    #except:#Skip if this cfradial netCDF file has Exception Error
    #    err_msg.write(filename+'\n')

    radar = pyart.io.read_cfradial(file_path)
    #If read 'radar' data successfully
    #Start extract metadata
    lat = radar.gate_latitude['data'][:]
    lon = radar.gate_longitude['data'][:]
    sec = radar.time['data'][:]

    filename = os.path.basename(file_path)
    tkn = filename.split('/')[-1].split('_')
    dt0 = datetime.strptime(f"{tkn[2]}{tkn[3]}",'%Y%m%d%H%M%S')

    st_time = dt0 + timedelta(seconds=int(min(sec))) 
    end_time = dt0 + timedelta(seconds=int(max(sec))) 

    write_to_dictionary(file_path, st_time, end_time, lat.max(), lat.min(), lon.max(), lon.min())


def write_to_dictionary(file_path, start, end, nlat, slat, elon, wlon):
    """
    Writes metadata information to dictionary
    :param file_path: Path to file being processed
    :param start: Start time of granule
    :param end: End time of granule
    :param nlat: Maximum latitude/ north
    :param slat: Minimum latitude/ south
    :param elon: Maximum longitude/ east
    :param wlon: Minimum longitude/ west
    """

    key = os.path.basename(file_path)
    metadata[key] = {}
    metadata[key]["temporal"] = [x.strftime('%Y-%m-%dT%H:%M:%SZ') for x in [start, end]]
    metadata[key]["wnes_geometry"] = [str(round(x,3)) for x in [wlon, nlat, elon, slat]]
    metadata[key]['SizeMBDataGranule'] = str(round(1E-6 * os.path.getsize(file_path), 2))
    with open(file_path, 'rb') as file:
        metadata[key]['checksum'] = md5(file.read()).hexdigest()
    metadata[key]['format'] = 'netCDF-4'

if __name__ == "__main__":
    #err_msg = open('bad_data_files_KBUF.txt','w')

    #print(f"Directory Path is {sys.argv[1]}")
    #file_dir = str(sys.argv[1])
    file_dir = '/ftp/ops/public/pub/fieldCampaigns/impacts/NEXRAD/KBUF/data/'

    for root, dirs, files in os.walk(file_dir):
        dirs.sort(reverse=True)
        for filename in sorted(files):
            #print(os.path.join(root, filename))
            get_metadata(os.path.join(root, filename))
    #print(metadata,file=err_msg)
    with open('../kbufimpactsRefData.json', 'w') as fp:
         json.dump(metadata, fp)

    #err_msg.close()
