from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset

class ExtractExrad3dimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract exrad3dimpacts
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.fileformat = 'netCDF-3'

        # extracting time and space metadata from nc file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):
        datafile = Dataset(self.file_path)
        lat = np.array(datafile['latitude'])
        lon = np.array(datafile['longitude'])
        #convert longitude values from [0,360] to [-180,180]
        lon = np.where(lon<=180.,lon,(lon-360.)) #if lon>180., convert it to lon-360.
        nlat, slat, elon, wlon = [np.nanmax(lat), np.nanmin(lat),
                                  np.nanmax(lon), np.nanmin(lon)]

        #IMPACTS_20200125_184910_191953_EXRAD_3dwinds.nc
        tkn = self.file_path.split('/')[-1].split('_')
        minTime, maxTime = [datetime.strptime(''.join([tkn[1],tkn[2]]),'%Y%m%d%H%M%S'),
                            datetime.strptime(''.join([tkn[1],tkn[3]]),'%Y%m%d%H%M%S')]

        datafile.close()
        return minTime, maxTime, slat, nlat, wlon, elon

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
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

    def get_metadata(self, ds_short_name, format='netCDF-3', version='1', **kwargs):
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
        data['AgeOffFlag'] = True if 'NRT' in data['GranuleUR'] else False
        return data
