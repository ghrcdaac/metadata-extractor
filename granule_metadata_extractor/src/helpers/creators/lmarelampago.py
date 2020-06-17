import os  # -- Needed for locating python metadata codebase
from datetime import datetime, timedelta
import json
import re
import sys
import tarfile
import shutil
import gzip
import h5py
import numpy as np
from netCDF4 import Dataset
from hashlib import md5

metadata = {}
network_metadata = {}
# network defaults
L3_NLat = -29.856252670288086
L3_SLat = -33.46372985839844
L3_WLon = -66.16654205322266
L3_ELon = -61.959259033203125


def get_tar_metadata(file_path):
    """
    Parse tar files for spatial and temporal metadata
    :param file_path: path to local tar file
    """
    print(file_path)
    min_lat, max_lat, min_lon, max_lon, min_time, max_time, lat, lon, dt = initialize_values()
    file_type_switcher = {
        "dat": get_ascii_metadata,
        "h5": get_hdf5_metadata,
        "nc": get_netcdf4_metadata
    }
    tar_opened = tarfile.open(file_path)
    file_name = os.path.basename(file_path)
    file_key = re.search('^RELAMP_LMA_(.*)_level[1-3].tar.gz', file_name)[1]
    for elem in tar_opened.getmembers():
        tar_opened.extract(path=os.path.join('tmp', f'{file_key}'), member=elem)
        granule_metadata = file_type_switcher.get(
            re.search('.*\.(.*)$', elem.get_info().get('name').rstrip('.gz'))[1])(file_key,
                                                                       elem.get_info().get('name'))
        [lat.append(x) for x in [granule_metadata['min_lat'], granule_metadata['max_lat']]]
        [lon.append(x) for x in [granule_metadata['min_lon'], granule_metadata['max_lon']]]
        [dt.append(x) for x in [granule_metadata['min_time'], granule_metadata['max_time']]]

    shutil.rmtree(os.path.join('tmp', f'{file_key}'))

    min_lat = max(L3_SLat, min(min_lat, min(lat)))
    max_lat = min(L3_NLat, max(max_lat, max(lat)))
    min_lon = max(L3_WLon, min(min_lon, min(lon)))
    max_lon = min(L3_ELon, max(max_lon, max(lon)))
    min_time = min(min_time, min(dt))
    max_time = max(max_time, max(dt))

    write_to_dictionary(file_path, min_time, max_time, max_lat, min_lat, max_lon, min_lon)
    shutil.rmtree('tmp')


def get_ascii_metadata(directory_key, filename):
    """
    Parse dat files for spatial and temporal metadata
    :param directory_key: Key for name of directory holding file
    :param filename: Name of file to be parsed
    :return: dictionary with granule spatial and temporal metadata
    """

    granule_metadata = {}
    lat = []
    lon = []
    network_lat = []
    network_lon = []

    granule_metadata['min_time'] = datetime.strptime(re.search('^RELAMP_LMA_(.*)_[0-9]{4}.dat.gz',
                                                               filename)[1], '%Y%m%d_%H%M%S')
    granule_metadata['max_time'] = granule_metadata['min_time'] + timedelta(minutes=10, seconds=-1)

    with gzip.open(os.path.join('tmp', directory_key, filename)) as f:
        lines = f.readlines()

    for line in lines:
        tkn = line.decode().split(':')
        if 'Sta_info' in tkn[0]:
            network_lat.append(float(tkn[1].split()[2]))
            network_lon.append(float(tkn[1].split()[3]))
        if 'Number of events' in tkn[0]:
            if int(tkn[1]) == 0:
                lat = network_lat
                lon = network_lon
        if '.' in tkn[0]:
            lat.append(float(line.split()[1]))
            lon.append(float(line.split()[2]))

    granule_metadata['max_lat'] = max(lat)
    granule_metadata['min_lat'] = min(lat)
    granule_metadata['max_lon'] = max(lon)
    granule_metadata['min_lon'] = min(lon)

    network_metadata[directory_key] = {}
    network_metadata[directory_key]["max_lat"] = max(network_lat)
    network_metadata[directory_key]["min_lat"] = min(network_lat)
    network_metadata[directory_key]["max_lon"] = max(network_lon)
    network_metadata[directory_key]["min_lon"] = min(network_lon)

    return granule_metadata


def get_hdf5_metadata(key, filename):
    """
    Parse hd5 files for spatial and temporal metadata
    :param key: Key for name of directory holding file and network metadata
    :param filename: Name of file to be parsed
    :return: dictionary with granule spatial and temporal metadata
    """
    granule_metadata = {}
    if filename in 'RELAMP_LMA_20181214_014001_0600.dat.flash.h5.gz':
        granule_metadata['min_time'] = datetime.strptime(
            re.search('^RELAMP_LMA_(.*)_[0-9]{4}.dat.flash.h5',
                      filename.rstrip('.gz'))[1], '%Y%m%d_%H%M%S')
    else:
        granule_metadata['min_time'] = datetime.strptime(
            re.search('^RELAMP_LMA_(.*)_[0-9]{4}.flash.h5',
                      filename.rstrip('.gz'))[1], '%Y%m%d_%H%M%S')
    granule_metadata['max_time'] = granule_metadata['min_time'] + timedelta(minutes=10, seconds=-1)

    temp_path_to_file = os.path.join('tmp', key, filename)
    if '.gz' in temp_path_to_file:
        with gzip.open(temp_path_to_file) as f_in:
            with open(temp_path_to_file.rstrip('.gz'), 'wb+') as f_out:
                f_out.write(f_in.read())

    tkn0 = filename.split('_')
    data = h5py.File(temp_path_to_file.rstrip('.gz'), 'r')
    events = np.array(data['events']['LMA_' + tkn0[2][2:8] + '_' + tkn0[3] + '_600'])
    flashes = np.array(data['flashes']['LMA_' + tkn0[2][2:8] + '_' + tkn0[3] + '_600'])
    data.close()

    if events.size > 0:
        lat = events['lat'][:].tolist() + flashes['init_lat'][:].tolist() + flashes['ctr_lat'][
                                                                            :].tolist()
        lon = events['lon'][:].tolist() + flashes['init_lon'][:].tolist() + flashes['ctr_lon'][
                                                                            :].tolist()
        granule_metadata['max_lat'] = max(lat)
        granule_metadata['min_lat'] = min(lat)
        granule_metadata['max_lon'] = max(lon)
        granule_metadata['min_lon'] = min(lon)
    else:
        granule_metadata['max_lat'] = network_metadata[key]['max_lat']
        granule_metadata['min_lat'] = network_metadata[key]['min_lat']
        granule_metadata['max_lon'] = network_metadata[key]['max_lon']
        granule_metadata['min_lon'] = network_metadata[key]['min_lon']

    return granule_metadata


def get_netcdf4_metadata(key, filename):
    """
    Parse netcdf4 files for spatial and temporal metadata
    :param key: Key for name of directory holding file and network metadata
    :param filename: Name of file to be parsed
    :return: dictionary with granule spatial and temporal metadata
    """

    granule_metadata = {}
    granule_metadata['min_time'] = datetime.strptime(
        re.search('^^RELAMP_LMA_(.*)_[0-9]{3}_.*.nc.gz',
                  filename)[1], '%Y%m%d_%H%M%S')
    granule_metadata['max_time'] = granule_metadata['min_time'] + timedelta(minutes=10, seconds=-1)

    with gzip.open(os.path.join('tmp', key, filename)) as gz:
        with Dataset('ignore_this_name', mode='r', memory=gz.read()) as dataset:
            lat = np.array(dataset.variables['latitude'])
            lon = np.array(dataset.variables['longitude'])

            granule_metadata['max_lat'] = max(lat)
            granule_metadata['min_lat'] = min(lat)
            granule_metadata['max_lon'] = max(lon)
            granule_metadata['min_lon'] = min(lon)

    return granule_metadata


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
    metadata[key]["wnes_geometry"] = [str(round(x, 3)) for x in [wlon, nlat, elon, slat]]
    metadata[key]['SizeMBDataGranule'] = str(round(1E-6 * os.path.getsize(file_path), 2))
    with open(file_path, 'rb') as file:
        metadata[key]['checksum'] = md5(file.read()).hexdigest()
    level = re.search('^RELAMP_LMA_[0-9]{8}_(.*).tar.gz', os.path.basename(file_path))[1]
    format_dict = {
        'level1': 'ASCII',
        'level2': 'HDF-5',
        'level3': 'netCDF-4'
    }
    metadata[key]['format'] = format_dict[level]


def initialize_values():
    """
    Initializes default values
    :return: List of default values- min_lat, max_lat, min_lon, max_lon, min_time, max_time
    """

    return [90, -90, 180, -180, datetime(2100, 1, 1), datetime(1900, 1, 1), [], [], []]


if __name__ == "__main__":
    print(f"Directory Path is {sys.argv[1]}")
    file_dir = str(sys.argv[1])
    for subdir, dirs, files in os.walk(file_dir):
        for file in sorted(files):
            get_tar_metadata(f"{os.path.join(file_dir, file)}")

    print(metadata)
    with open('../lmarelampagoRefData.json', 'w') as fp:
        json.dump(metadata, fp)
