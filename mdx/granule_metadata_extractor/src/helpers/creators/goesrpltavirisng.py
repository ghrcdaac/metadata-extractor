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
#                                                 recreate goesrpltavirisngRefData.json

metadata = {}


def get_metadata(file_path):
    """
    Parse tar files for spatial and temporal metadata
    :param file_path: path to local tar file
    """
    filename = os.path.basename(file_path)
    granule_metadata = get_tar_metadata(filename, file_path) if re.search('[0-9].tar.gz$',
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
    maxlat, minlat, maxlon, minlon, time_arr, dtstr = [0 for x in range(0, 6)]
    tar_opened = tarfile.open(file_path)
    elem_path = os.path.join('/tmp', 'goesrpltavirisng')
    for elem in sorted(tar_opened.getmembers(), key=lambda m: m.name):
        elem_name = elem.get_info().get('name')
        if re.search('(_rdn_obs|_rdn_loc)$', elem_name) is not None:
            tar_opened.extract(path=elem_path, member=elem)
            os.chmod(os.path.join(elem_path, elem_name), 0o600)
        if re.search('_rdn_obs.hdr$', elem_name) is not None:
            tar_opened.extract(path=elem_path, member=elem)
            os.chmod(os.path.join(elem_path, elem_name), 0o600)
            time_arr = numpy.array(get_loc_mm(os.path.join(elem_path, elem_name))[:, :, 9])
            dtstr = re.findall('[0-9]{8}', filename)[0]
        if re.search('_rdn_loc.hdr', elem_name) is not None:
            tar_opened.extract(path=elem_path, member=elem)
            os.chmod(os.path.join(elem_path, elem_name), 0o600)
            maxlat, minlat, maxlon, minlon = get_spatial_metadata(
                get_loc_mm(os.path.join(elem_path, elem_name)))

    shutil.rmtree(elem_path)

    granule_metadata['wnes_geometry'] = [str(round(x, 3)) for x in
                                         [minlon, maxlat, maxlon, minlat]]
    start_time, end_time = [x.strftime('%Y-%m-%dT%H:%M:%SZ') for x in
                            [datetime.strptime(dtstr, '%Y%m%d') +
                             timedelta(hours=time_arr.min()), datetime.strptime(dtstr, '%Y%m%d') +
                             timedelta(hours=time_arr.max())]]
    granule_metadata['temporal'] = [start_time, end_time]

    return granule_metadata


def get_loc_mm(file_path):
    """
    Returns granule memmap to use for finding spatial and temporal metadata of granule
    :param file_path: Path to obtain memmap from
    :return: granule memmap
    """
    loc_meta = envi.read_envi_header(file_path)
    loc_shape = (int(loc_meta['lines']), int(loc_meta['samples']), int(loc_meta['bands']))
    loc_mm = numpy.memmap(file_path.rstrip('.hdr'), mode='r+', shape=loc_shape, dtype='float64')
    return loc_mm


def get_spatial_metadata(loc_mm):
    """
    Parse tar files for spatial and temporal metadata
    :param loc_mm: Granule memmap
    :return: list with granule spatial metadata
    """

    lon_arr = numpy.array(loc_mm[:, :, 0])
    lat_arr = numpy.array(loc_mm[:, :, 1])

    # -- there are some latitude value = 0.0 and need to be filtered out
    maxlat = -90.0
    minlat = 90.0
    minlat = min(minlat, numpy.min(lat_arr))
    maxlat = max(maxlat, numpy.max(lat_arr))

    return [maxlat, minlat, lon_arr.max(), lon_arr.min()]


def get_reference_metadata(filename):
    """
    Parse ortho L2 tar, clip, and clip.hdr files for spatial and temporal metadata
    :param filename: Name of tar or clip file to be processed
    :return: dictionary with granule spatial and temporal metadata
    """
    reference_key = f"{re.findall('.*[0-9]{8}t[0-9]{6}', filename)[0]}.tar.gz"
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
    format_swicher = {
        "lip": "Binary",
        "hdr": "ASCII"
    }
    metadata[key]['format'] = format_swicher.get(re.search('...$', file_path)[0], 'Various: Binary, ASCII')


if __name__ == "__main__":
    print(f"Directory Path is {sys.argv[1]}")
    file_dir = str(sys.argv[1])

    # for subdirs, dirs, files in os.walk(file_dir):
    #     for file in sorted(files):
    #         print(f"{os.path.join(file_dir, subdirs, file)}")
            # get_metadata(f"{os.path.join(file_dir, file)}")

    for root, dirs, files in os.walk(file_dir):
        dirs.sort()
        for filename in sorted(files):
            print(os.path.join(root, filename))
            get_metadata(os.path.join(root, filename))

    # print(metadata)
    with open('../goesrpltavirisngRefData.json', 'w') as fp:
        json.dump(metadata, fp)
