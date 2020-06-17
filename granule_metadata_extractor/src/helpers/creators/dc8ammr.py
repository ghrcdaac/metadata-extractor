import os  # -- Needed for locating python metadata codebase
from datetime import datetime, timedelta
import json
import re
import sys

metadata = {}


def read_DC8NAV_metadata(file_path):
    """
    Parses dc8nav files for temporal and spatial metadata and stores to metadata dict
    :param file_path: path to the file to be parsed
    """
    if '_flt.txt' not in file_path:
        return

    minlat, maxlat, minlon, maxlon, stTime, endTime, lat, lon, dt_list = initialize_values()
    doy_from_filename = int(os.path.basename(file_path).split('.')[0][2:5])

    with open(file_path, 'r') as f:
        file_lines = f.readlines()

    for line in file_lines:
        if re.search("^C", line) is None:
            continue

        tkn = line.split()
        doy = int(tkn[1])

        if doy != doy_from_filename and doy != doy_from_filename + 1:
            continue

        dt = datetime.strptime(f"19980101T{tkn[2]}", "%Y%m%dT%H:%M:%S.%f") + timedelta(
            days=doy - 1)

        clat, clon = [float(x) for x in [tkn[3], tkn[5]]]
        lat_offset, lon_offset = [float(x) / 60.0 for x in [tkn[4], tkn[6]]]
        clat += lat_offset if clat >= 0 else -lat_offset
        clon += lon_offset if clon >= 0 else -lon_offset

        lat.append(round(clat, 3))
        lon.append(round(clon, 3))
        dt_list.append(dt)

    key = f"1998{doy_from_filename}"
    metadata[key] = {}
    metadata[key]["NLat"] = max(maxlat, max(lat))
    metadata[key]["SLat"] = min(minlat, min(lat))
    metadata[key]["ELon"] = max(maxlon, max(lon))
    metadata[key]["WLon"] = min(minlon, min(lon))
    metadata[key]["start"] = min(stTime, min(dt_list)).strftime('%Y-%m-%dT%H:%M:%SZ')
    metadata[key]["end"] = max(endTime, max(dt_list)).strftime('%Y-%m-%dT%H:%M:%SZ')


def initialize_values():
    """
    Initializes default values
    :return: List of default values- minLat, maxLat, minLon, maxLon, minTime, maxTime, lat, lon
    """

    return [90, -90, 180, -180, datetime(2100, 1, 1), datetime(1900, 1, 1), [], [], []]


if __name__ == "__main__":
    print(f"Directory Path is {sys.argv[1]}")
    file_dir = str(sys.argv[1])
    for subdir, dirs, files in os.walk(file_dir):
        for filename in files:
            read_DC8NAV_metadata(f"{os.path.join(file_dir, filename)}")

    print(metadata)
    with open('../dc8ammrRefData.json', 'w') as fp:
        json.dump(metadata, fp)
