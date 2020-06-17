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
#                                                 recreate gpmikalpvexRefData.json

import pyart

metadata = {}


def get_metadata(file_path):
    """
    Parse tar files for spatial and temporal metadata
    :param file_path: path to local tar file
    """
    filename = os.path.basename(file_path)
    granule_metadata = get_tar_metadata(filename, file_path) if re.search('_UF_',
                                                                          filename) else get_reference_metadata(
        filename)
    write_to_dictionary(file_path, granule_metadata.get('temporal'),
                        granule_metadata.get('wnes_geometry'))


def get_tar_metadata(filename, file_path):
    """
    Parse tar files for spatial and temporal metadata
    :param filename: Name of tar file being processed
    :param file_path: Path to file to be parsed
    :return: dictionary with granule spatial and temporal metadata
    """
    granule_metadata = {}
    maxlat = -90.
    minlat = 90.
    maxlon = -180.
    minlon = 180.
    mintime = datetime(2100,1,1)
    maxtime = datetime(1900,1,1)

    tar_opened = tarfile.open(file_path)
    elem_path = os.path.join('/tmp', 'gpmikalpvex')
    for elem in sorted(tar_opened.getmembers(), key=lambda m: m.name):
        elem_name = elem.get_info().get('name')

        tar_opened.extract(path=elem_path, member=elem)
        os.chmod(os.path.join(elem_path, elem_name), 0o600)

        try:
            radar = pyart.io.read_uf(os.path.join(elem_path, elem_name))
        except: #Skip if this UF file has Exception Error
            #err_msg.write(fname+'\n')
            continue #Go to next UF file

        #If read 'radar' data successfully
        #Start extract metadata
        lat = radar.gate_latitude['data'][:]
        lon = radar.gate_longitude['data'][:]
        sec = radar.time['data'][:]

        if '_IKA.' in elem_name:
        #for IKA dataset, i.e., 201010190700_IKA.PPI1_A.raw.uf
           dt0 =datetime.strptime(elem_name.split('_')[0][:],'%Y%m%d%H%M%S')
        else:
        #for datasets except IKA
           dt0 =datetime.strptime('20'+elem_name.split('.')[0][3:],'%Y%m%d%H%M%S')

        start = dt0 + timedelta(seconds=int(min(sec)))
        end = dt0 + timedelta(seconds=int(max(sec)))

        nlat = lat.max()
        slat = lat.min()
        elon = lon.max()
        wlon = lon.min()

        if start < mintime:
            mintime = start
        if end > maxtime:
            maxtime = end
        if slat < minlat:
            minlat = slat
        if nlat > maxlat:
            maxlat = nlat
        if wlon < minlon:
            minlon = wlon
        if elon > maxlon:
            maxlon = elon

    shutil.rmtree(elem_path)

    granule_metadata['wnes_geometry'] = [str(round(x, 3)) for x in
                                         [minlon, maxlat, maxlon, minlat]]
    start_time = datetime.strftime(mintime, '%Y-%m-%dT%H:%M:%SZ')
    end_time = datetime.strftime(maxtime, '%Y-%m-%dT%H:%M:%SZ')
    granule_metadata['temporal'] = [start_time, end_time]

    return granule_metadata


def get_reference_metadata(filename):
    """
    Parse '_RAW_' (Binary) files for spatial and temporal metadata
    :param filename: Name of tar or clip file to be processed
    :return: dictionary with granule spatial and temporal metadata
    """
    #reference_key = f"{re.findall('.*[0-9]{8}t[0-9]{6}', filename)[0]}.tar.gz"
    #lpvex_RADAR_IKAALINEN_UF_20101019.tar.gz
    reference_key = re.sub('_RAW_','_UF_',filename)
    granule_metdata = {'wnes_geometry': metadata[reference_key]['wnes_geometry'],
                       'temporal': metadata[reference_key]['temporal']}

    return granule_metdata


def write_to_dictionary(file_path, temporal, wnes_geometry):
    """
    Writes metadata information to dictionary
    :param file_path: Path to file being processed
    :param temporal: List containing granule temporal information [start time, end time]
    :param wnes_geometry: List containing granule spatial information [west, north, east, south]
    """

    key = os.path.basename(file_path)
    metadata[key] = {}
    metadata[key]["temporal"] = temporal
    metadata[key]["wnes_geometry"] = wnes_geometry
    metadata[key]['SizeMBDataGranule'] = str(round(1E-6 * os.path.getsize(file_path), 2))
    with open(file_path, 'rb') as file:
        metadata[key]['checksum'] = md5(file.read()).hexdigest()
    format_switcher = {
        "UF": "Universal Format (UF)",
        "RAW": "Binary"
    }
    metadata[key]['format'] = format_switcher.get(re.search('^lpvex_RADAR_IKAALINEN_(.*)_\d{8}.tar.gz$', file_path.split('/')[-1])[1])

if __name__ == "__main__":
    #print(f"Directory Path is {sys.argv[1]}")
    #file_dir = str(sys.argv[1])
    #Extract metadata from UF files first, and assgin UF metadata to RAW binary files
    file_dir = '/ftp/public/pub/fieldCampaigns/gpmValidation/lpvex/C-band_radar/IKA/data/'
    print(file_dir)

    for root, dirs, files in os.walk(file_dir):
        dirs.sort(reverse=True)
        for filename in sorted(files):
            print(os.path.join(root, filename))
            get_metadata(os.path.join(root, filename))

    #print(metadata)
    with open('../gpmikalpvexRefData.json', 'w') as fp:
        json.dump(metadata, fp)
