import json
import os
import re
import shutil
import sys
import tarfile
from datetime import datetime, timedelta
from hashlib import md5
import numpy
#from spectral.io import envi                    # this library is only needed if you intend to
                                                 #recreate [ds_short_name]RefData.json

#bounding box for the dataset is from dataset landing page
# https://ghrc.nsstc.nasa.gov/hydro/details/gpmkdmx2ifld
nlat = 45.87
slat = 37.59
elon = -88.18
wlon = -99.26

metadata = {}
#load renaming script to figure file mapping
def load_renaming_script(script_name):
    rn_files = [] #a list of dictionaries
    fp = open(script_name)
    for line in fp:
    #i.e., mv /ftp/ops/public/pub/fieldCampaigns/gpmValidation/ifloods/NEXRAD2/KARX/data/2013-03-30/Level2_KARX_20130329_2351.ar2v /ftp/ops/public/pub/fieldCampaigns/gpmValidation/ifloods/NEXRAD2/KARX/data/2013-03-30/Level2_KARX_20130330_0000.ar2v
        old_file = line.split()[1].split('/')[-1]
        new_file = line.split()[-1].split('/')[-1]
        rn_files.append({'old': old_file, 'new': new_file})
    fp.close()

    old_files = [x['old'] for x in rn_files]
    new_files = [x['new'] for x in rn_files]

    return rn_files, old_files, new_files

def get_metadata(file_path):
    filename = os.path.basename(file_path)
    fname = filename.split('/')[-1] #i.e.,Level2_KARX_20130430_0310.ar2v
    tkn = fname.split('.')[0].split('_')

    if fname in new_files:
       old_fname = [x['old'] for x in rn_files if x['new'] == fname][0]
       old_meta = [x for x in dbmeta if x['granule_name'] == old_fname][0]

       st_time = datetime.strptime(''.join(tkn[2:4]),'%Y%m%d%H%M')
       end_time  = datetime.strptime(old_meta['stop_date'],'%Y-%m-%d %H:%M:%S')
    elif fname in old_files:
       old_meta = [x for x in dbmeta if x['granule_name'] == fname][0]
       st_time = datetime.strptime(old_meta['start_date'],'%Y-%m-%d %H:%M:%S')
       end_time = datetime.strptime(tkn[2],'%Y%m%d') + timedelta(seconds=86399)
    else:
       old_meta = [x for x in dbmeta if x['granule_name'] == fname][0]
       st_time = datetime.strptime(old_meta['start_date'],'%Y-%m-%d %H:%M:%S')
       end_time = datetime.strptime(old_meta['stop_date'],'%Y-%m-%d %H:%M:%S')

    write_to_dictionary(file_path, st_time, end_time, nlat, slat, elon, wlon)

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
    metadata[key]['format'] = 'Binary'

if __name__ == "__main__":
    file_dir = "/ftp/ops/public/pub/fieldCampaigns/gpmValidation/ifloods/NEXRAD2/KDMX/data/"
    #(1) read in metadata Mary extracted from database
    with open('gpmkdmx2ifld_dbmeta_redone','r') as fp:
         dbmeta = json.load(fp)

    #(2) read the list of renamed files
    rn_files, old_files, new_files = load_renaming_script('KDMX_renaming_script')
    for root, dirs, files in os.walk(file_dir):
        dirs.sort(reverse=True)
        for filename in sorted(files):
            get_metadata(os.path.join(root, filename))

    with open('../gpmkdmx2ifldRefData.json', 'w') as fp:
         json.dump(metadata, fp)
