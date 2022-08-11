from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy as np
from netCDF4 import Dataset
from datetime import datetime, timedelta

class ExtractScrxsondecpexawMetadata(ExtractNetCDFMetadata):
    """
    A class to extract scrxsondecpexaw
    """

    def __init__(self, file_path):
        self.file_path = file_path
        # these are needed to metadata extractor
        if file_path.endswith('.nc'):
           self.fileformat = 'netCDF-4'

           # extracting time and space metadata for netcdf/ascii file
           [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
               self.get_variables_min_max_netcdf()
        else: #ascii (.txt)
           self.fileformat = 'ASCII'

           # extracting time and space metadata for netcdf/ascii file
           [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
               self.get_variables_min_max_ascii()

    def get_variables_min_max_netcdf(self):
        """
        :return:
        """
        data = Dataset(self.file_path)
        ref_time = datetime.strptime(data.start_date_and_time,'%Y/%m/%d %H:%M:%S') #i.e.,'2021/08/19 23:12:00'
        sec = data['STANDARDVARS/TIME'][:] #seconds since start_date_and_time
        lon = data['STANDARDVARS/LONG'][:] #degreeE
        lat = data['STANDARDVARS/LATI'][:] #degreeN
        data.close()

        minTime = ref_time + timedelta(seconds = min(sec))
        maxTime = ref_time + timedelta(seconds = max(sec))
        maxlat, minlat, maxlon, minlon = [max(lat),
                                          min(lat),
                                          max(lon),
                                          min(lon)]

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_variables_min_max_ascii(self):
        """
        :return:
        """

        fname = self.file_path.split('/')[-1]
        if '_unix_' in fname:
           #Sample file SCRX_Radiosonde_CPEXAW_unix_20210830_1301.txt
           key = fname.split('.')[0].split('_CPEXAW_unix_')[-1] #i.e.,20210819_2312
           with open(self.file_path,'r') as f:
                lines = f.readlines()
        else: #'_win_' in fname
           key = fname.split('.')[0].split('_CPEXAW_win_')[-1] #i.e.,20210819_2312
           with open(self.file_path,'rb') as f:
                lines0 = f.readlines()
           lines = [x.decode('utf-8') for x in lines0[1:]]
           # using insert() to append at beginning
           lines.insert(0,lines0[0])

        ref_time = datetime.strptime(key,'%Y%m%d_%H%M')

        sec = []
        lat = []
        lon = []
        for row in lines[1:]:
            tkn = row.split('\t')
            if '-----' not in tkn[6] and '-----' not in tkn[7]:
               sec.append(float(tkn[0]))
               lon.append(float(tkn[6]))
               lat.append(float(tkn[7]))

        minTime = ref_time + timedelta(seconds = min(sec))
        maxTime = ref_time + timedelta(seconds = max(sec))
        maxlat, minlat, maxlon, minlon = [max(lat),
                                          min(lat),
                                          max(lon),
                                          min(lon)]

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
        data['DataFormat'] = self.fileformat
        data['VersionId'] = version
        return data
