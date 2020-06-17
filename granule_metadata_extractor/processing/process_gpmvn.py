from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import json
import os
import pathlib
import gzip
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset
import shutil
import tempfile



class ExtractGpmvnMetadata(ExtractNetCDFMetadata):
    """
    A class to extract gpmvn
    """

    def __init__(self, file_path):
        # global gpmseafluxicepop_loc, north, south, east, west, start_time, end_time
        #super().__init__(file_path)
        self.file_path = file_path

        # extracting time and space metadata from nc.gz file
        infile = gzip.open(file_path, 'rb')
        tmp = tempfile.NamedTemporaryFile(delete=False)
        shutil.copyfileobj(infile, tmp)
        infile.close()
        tmp.close()
        dataset = Dataset(tmp.name)
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max(dataset, file_path)
        dataset.close()
        os.unlink(tmp.name)
        # for the 9 files without valid bounding box, just check SLat == -888.0  
        # will find one of the 9 files
        #if self.SLat == -888.0:
        if self.SLat < -90.0:
            [self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max_lookup(file_path) 

    def get_variables_min_max_lookup(self, file_path):
        # there are 9 files that do not have valid lat and lon and therefore no valid
        # bounding box. Leigh and I have decided to use site-specific collection
        # metadata for these files. This stats are calculated offline
        lut = {'KAPX_MS': [43.97828, 45.83322, -86.03121, -83.3946], \
               'KBRO_HS': [24.97985, 26.84472, -98.44991, -96.36781], \
               'KFWS_HS': [31.63203, 33.51094, -98.44468, -96.17033], \
               'KILN_MS': [38.50105, 40.33742, -85.03599, -82.61572], \
               'KINX_HS': [35.24335, 37.12228, -96.76854, -94.38538], \
               'KMQT_HS': [45.58255, 47.49397, -88.94563, -86.16524], \
               'KMQT_MS': [45.61153, 47.44971, -88.91314, -86.18689], \
               'KTBW_MS': [26.78414, 28.62319, -83.45336, -81.33512], \
               'PAIH_HS': [58.43881, 61.54339, -149.3945, -143.3543]}
        tkn = file_path.split('/')[-1].split('.')
        key = f"{tkn[1]}_{tkn[6]}"
        
        return lut[key]

    def get_variables_min_max(self, nc, file_path):
        """
        :param nc: Dataset opened
        :param file_path: file path
        :return:
        """

        # get latitude and longitude variables
        if file_path.find('GRtoDPRGMI') >= 0:
            latbuf = np.array(nc.variables['latitude_NS'])
            lonbuf = np.array(nc.variables['longitude_NS'])
        else:
            latbuf = np.array(nc.variables['latitude'])
            lonbuf = np.array(nc.variables['longitude'])
        # get time variable
        timebuf = np.array(nc.variables['timeSweepStart'])

        # get time range
        minsec, maxsec = [np.min(timebuf), np.max(timebuf)]
        # reference time is 1/1/1970
        reftime = datetime(1970, 1, 1)
        minTime, maxTime = [reftime + timedelta(seconds=minsec), reftime + timedelta(seconds=maxsec)]

        # get bounding box as minlat, maxlat, minlon, maxlon
        minlat, maxlat, minlon, maxlon = [np.min(latbuf), np.max(latbuf), np.min(lonbuf), np.max(lonbuf)]

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a GIF file
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    [self.NLat, self.SLat, self.ELon, self.WLon]]
        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]

    def get_temporal(self, time_variable_key='time', units_variable='units', scale_factor=1.0,
                     offset=0,
                     date_format='%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The NetCDF variable we need to target
        :param units_variable: The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """
        start_date = self.minTime.strftime(date_format)
        stop_date = self.maxTime.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, format='netCDF-4', version='1', **kwargs):
        """
        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        data = dict()
        data['GranuleUR'] = granule_name = os.path.basename(self.file_path)
        start_date, stop_date = self.get_temporal()
        data['ShortName'] = ds_short_name
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry()
        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in geometry_list)
        data['checksum'] = self.get_checksum()
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['DataFormat'] = 'netCDF-3'
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Gpmvn Metadata')
    path_to_file = "../../test/fixtures/GRtoDPR.KMQT.150804.8127.V06A.DPR.MS.1_21.nc.gz"
    exnet = ExtractGpmvnMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
