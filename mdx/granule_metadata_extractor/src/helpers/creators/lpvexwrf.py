import os
from datetime import datetime, timedelta
import gzip
from netCDF4 import Dataset
import json
import sys
from hashlib import md5

metadata = {}


def initialize_file(file_path):
    """
    Unzips and reads files. Passes parameters through assignment pipeline
    :param string file_path: path to file to be initialized
    """
    min_lat, max_lat, min_lon, max_lon, st_time, end_time = initialize_values()
    with gzip.open(file_path) as gz:
        with Dataset('ignore_this_name', mode='r', memory=gz.read()) as dataset:
            st_time, end_time = get_temporal(dataset, st_time, end_time)
            min_lat, max_lat, min_lon, max_lon = get_spatial(dataset, min_lat, max_lat, min_lon,
                                                             max_lon)
            write_to_dictionary(file_path, st_time, end_time, max_lat, min_lat, max_lon, min_lon)


def get_temporal(dataset, st_time, end_time):
    """
    Gets starts and end time from netcdf dataset
    :param netCDF4.Dataset dataset: dataset holding information read from netCDF file
    :param datetime st_time: default start time of collection
    :param datetime end_time: default end time of collection
    :return: List containing both start and end time of granule
    """
    st_time = min(st_time,
                  datetime.strptime(dataset.variables['Times'][0].tostring().decode("utf-8"),
                                    '%Y-%m-%d_%H:%M:%S'))
    end_time = max(end_time, st_time + timedelta(hours=1))
    return [st_time, end_time]


def get_spatial(dataset, min_lat, max_lat, min_lon, max_lon):
    """
    Gets minimum/maximum latitude/longitude from netcdf dataset
    :param netCDF4.Dataset dataset: dataset holding information read from netCDF file
    :param int min_lat: default minimum latitude of collection
    :param int max_lat: default maximum latitude of collection
    :param int min_lon: default minimum longitude of collection
    :param int max_lon: default maximum longitude of collection
    :return: List containing minimum/maximum latitude/longitude of granule
    """
    for x in dataset.variables['XLAT'][0].data:
        min_lat = min(min_lat, min(x))
        max_lat = max(max_lat, max(x))
    for x in dataset.variables['XLONG'][0].data:
        min_lon = min(min_lon, min(x))
        max_lon = max(max_lon, max(x))
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


def initialize_values():
    """
    Initializes default values
    :return: List of default values- minLat, maxLat, minLon, maxLon, minTime, maxTime, lat, lon
    """

    return [90, -90, 180, -180, datetime(2100, 1, 1), datetime(1900, 1, 1)]


if __name__ == "__main__":
    print(f"Directory Path is {sys.argv[1]}")
    file_dir = str(sys.argv[1])
    for subdir, dirs, files in os.walk(file_dir):
        for filename in files:
            initialize_file(f"{os.path.join(file_dir, filename)}")

    print(metadata)
    with open('../lpvexwrfRefData.json', 'w') as fp:
        json.dump(metadata, fp)
