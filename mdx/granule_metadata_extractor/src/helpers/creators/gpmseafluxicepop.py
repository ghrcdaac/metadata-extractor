import os
from datetime import datetime, timedelta
import gzip
from netCDF4 import Dataset
import json
import sys
from hashlib import md5
import numpy as np

metadata = {}


def initialize_file(file_path):
    """
    Unzips and reads files. Passes parameters through assignment pipeline
    :param string file_path: path to file to be initialized
    """
    with gzip.open(file_path) as gz:
        with Dataset('ignore_this_name', mode='r', memory=gz.read()) as dataset:
            st_time, end_time = get_temporal(dataset)
            min_lat, max_lat, min_lon, max_lon = get_spatial(dataset)
            write_to_dictionary(file_path, st_time, end_time, max_lat, min_lat, max_lon, min_lon)


def get_temporal(dataset):
    """
    Gets starts and end time from netcdf dataset
    :param netCDF4.Dataset dataset: dataset holding information read from netCDF file
    :return: List containing both start and end time of granule
    """

    timebuf = np.array(dataset.variables['time'])
    stsec, endsec = [np.min(timebuf), np.max(timebuf)]

    st_time = datetime(1970, 1, 1) + timedelta(seconds=stsec)
    end_time = datetime(1970, 1, 1) + timedelta(seconds=endsec)

    return [st_time, end_time]


def get_spatial(dataset):
    """
    Gets minimum/maximum latitude/longitude from netcdf dataset
    :param netCDF4.Dataset dataset: dataset holding information read from netCDF file
    :return: List containing minimum/maximum latitude/longitude of granule
    """
    latbuf = np.array(dataset.variables['lat_bnds'])
    lonbuf = np.array(dataset.variables['lon_bnds'])
    min_lat, max_lat = [np.min(latbuf), np.max(latbuf)]
    min_lon, max_lon = [np.min(lonbuf), np.max(lonbuf)]

    return [str(round(x, 3)) for x in [min_lat, max_lat, min_lon, max_lon]]


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
    metadata[key]["wnes_geometry"] = [wlon, nlat, elon, slat]
    metadata[key]['SizeMBDataGranule'] = str(round(1E-6 * os.path.getsize(file_path), 2))
    with open(file_path, 'rb') as file:
        metadata[key]['checksum'] = md5(file.read()).hexdigest()
    metadata[key]['format'] = 'netCDF-4'

if __name__ == "__main__":
    print(f"Directory Path is {sys.argv[1]}")
    file_dir = str(sys.argv[1])
    for subdir, dirs, files in os.walk(file_dir):
        for filename in files:
            initialize_file(f"{os.path.join(file_dir, filename)}")

    print(metadata)
    with open('../gpmseafluxicepopRefData.json', 'w') as fp:
        json.dump(metadata, fp)
