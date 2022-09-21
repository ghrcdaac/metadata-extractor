import os  # -- Needed for locating python metadata codebase
from datetime import datetime, timedelta
import json
import re
import sys

metadata = {'98220': {}, '98227': {}, '98235': {}, '98236': {}, '98238': {},
            '98242': {}, '98245': {}, '98248': {}, '98251': {}}

metadata['98220']['start'] = datetime(1998, 8, 8, 17, 33, 0)
metadata['98220']['end'] = datetime(1998, 8, 8, 17, 52, 0)

metadata['98227']['start'] = datetime(1998, 8, 15, 22, 22, 0)
metadata['98227']['end'] = datetime(1998, 8, 15, 22, 41, 0)

metadata['98235']['start'] = datetime(1998, 8, 23, 19, 49, 0)
metadata['98235']['end'] = datetime(1998, 8, 23, 20, 8, 0)

metadata['98236']['start'] = datetime(1998, 8, 24, 3, 55, 0)
metadata['98236']['end'] = datetime(1998, 8, 24, 4, 14, 0)

metadata['98238']['start'] = datetime(1998, 8, 26, 13, 11, 0)
metadata['98238']['end'] = datetime(1998, 8, 26, 13, 31, 0)

metadata['98242']["start"] = datetime(1998, 8, 30, 19, 40, 0).strftime('%Y-%m-%dT%H:%M:%SZ')
metadata['98242']["end"] = datetime(1998, 8, 30, 23, 24, 0).strftime('%Y-%m-%dT%H:%M:%SZ')
metadata['98242']["NLat"] = 24.62 + 1.5
metadata['98242']["SLat"] = 24.62 - 1.5
metadata['98242']["ELon"] = -78.02 + 1.5
metadata['98242']["WLon"] = -78.02 - 1.5

metadata['98245']["start"] = datetime(1998, 9, 2, 22, 0, 0).strftime('%Y-%m-%dT%H:%M:%SZ')
metadata['98245']["end"] = datetime(1998, 9, 3, 1, 5, 0).strftime('%Y-%m-%dT%H:%M:%SZ')
metadata['98245']["NLat"] = 30.4
metadata['98245']["SLat"] = 27.40
metadata['98245']["ELon"] = -82.4
metadata['98245']["WLon"] = -85.5

metadata['98248']['start'] = datetime(1998, 9, 5, 20, 47, 0)
metadata['98248']['end'] = datetime(1998, 9, 5, 21, 7, 0)

metadata['98251']['start'] = datetime(1998, 9, 8, 20, 31, 0)
metadata['98251']['end'] = datetime(1998, 9, 8, 20, 51, 0)


def initialize_file(file_path):
    """
    Initializes file by parsing file_path, testing that key is in our collection, opens the file,
    and passes the key and file contents to the appropriate method
    :param file_path: path to the file to be parsed
    :return:
    """
    dtstr = os.path.basename(file_path).split('.')[0].split('_')[-1][:5]

    if dtstr not in metadata.keys():
        return

    with open(file_path, 'r') as f:
        file_lines = f.readlines()

    if '.flt_data.txt' in file_path:
        read_flt_metadata(dtstr, file_lines)
    elif '.nastnav.txt' in file_path:
        read_nastnav_metadata(dtstr, file_lines)


def read_flt_metadata(dtstr, file_lines):
    """
    Parses flt files for temporal and spatial metadata
    :param dtstr: Date string used as key in dict
    :param file_lines: Content of the file stored as list of lines
    """

    minLat, maxLat, minLon, maxLon, minTime, maxTime, lat, lon = initialize_values(dtstr)

    for line in file_lines:
        if re.search("^[0-9]{3} ", line) is None:
            continue

        doy, hh, mm, ss = [int(x) for x in [line[0:3], line[4:6], line[7:9], line[10:12]]]
        lat_sign, lon_sign = [line[16:17], line[25:26]]
        clat, clon = [float(x) for x in [line[17:23], line[26:33]]]

        dt = datetime(1998, 1, 1, hh, mm, ss) + timedelta(days=doy - 1)

        if maxTime >= dt >= minTime:
            clat = -clat if 'S' in lat_sign else clat
            clon = -clon if 'W' in lon_sign else clon

            lat.append(clat)
            lon.append(clon)

    write_to_dictionary(dtstr, minTime, maxTime, max(maxLat, max(lat)), min(minLat, min(lat)),
                        max(maxLon, max(lon)), min(minLon, min(lon)))


def read_nastnav_metadata(dtstr, file_lines):
    """
    Parses nastnav files for temporal and spatial metadata
    :param dtstr: Date string used as key in dict
    :param file_lines: Content of the file stored as list of lines
    """

    minLat, maxLat, minLon, maxLon, minTime, maxTime, lat, lon = initialize_values(dtstr)

    for line in file_lines:
        tkn = line.split()
        if re.search("^#", line) is not None or len(tkn) < 6:
            continue

        hh, clat, clon = [float(x) for x in [tkn[1], tkn[4], tkn[6]]]

        dt = datetime(1998, 1, 1) + timedelta(days=int(dtstr[2:]) - 1) + timedelta(hours=hh)
        dt -= timedelta(days=1) if hh > 20.0 else dt

        lat.append(clat)
        lon.append(clon)

    write_to_dictionary(dtstr, minTime, maxTime, max(maxLat, max(lat)), min(minLat, min(lat)),
                        max(maxLon, max(lon)), min(minLon, min(lon)))


def write_to_dictionary(dtstr, start, end, nlat, slat, elon, wlon):
    """
    Writes metadata information to dictionary
    :param dtstr: Date string used as key in dict
    :param start: Start time of granule
    :param end: End time of granule
    :param nlat: Maximum latitude/ north
    :param slat: Minimum latitude/ south
    :param elon: Maximum longitude/ east
    :param wlon: Minimum longitude/ west
    """

    metadata[dtstr]["start"] = start.strftime('%Y-%m-%dT%H:%M:%SZ')
    metadata[dtstr]["end"] = end.strftime('%Y-%m-%dT%H:%M:%SZ')
    metadata[dtstr]["NLat"] = nlat
    metadata[dtstr]["SLat"] = slat
    metadata[dtstr]["ELon"] = elon
    metadata[dtstr]["WLon"] = wlon


def initialize_values(dtstr):
    """
    Initializes default values
    :param dtstr: Date string used as key in dict
    :return: List of default values- minLat, maxLat, minLon, maxLon, minTime, maxTime, lat, lon
    """

    return [90, -90, 180, -180, metadata[dtstr]['start'], metadata[dtstr]['end'], [], []]


if __name__ == "__main__":
    print(f"Directory Path is {sys.argv[1]}")
    file_dir = str(sys.argv[1])
    for subdir, dirs, files in os.walk(file_dir):
        for filename in files:
            initialize_file(f"{os.path.join(file_dir,filename)}")

    print(metadata)
    with open('../er2mirRefData.json', 'w') as fp:
        json.dump(metadata, fp)
