from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os, re
from datetime import datetime, timedelta
from netCDF4 import Dataset

class ExtractGpmpalMetadata(ExtractNetCDFMetadata):
    """
    A class to extract gpmpal metadata 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.fileformat = 'netCDF-4'

        # extracting time and space metadata from .nc file

        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        :param radar: Dataset opened
        :return:
        """
        data = Dataset(self.file_path)
        #examples:
        #geospatial_lat_min:   47.450
        #geospatial_lat_max:   53.335
        #geospatial_lon_min: -137.569
        #geospatial_lon_max: -126.118
        #geospatial_lat_bounds: POLYGON [-137.569, -126.118,   47.450,   53.335]
        #time_coverage_start: 2010-10-18 00:00:00UTC
        #time_coverage_end: 2014-11-13 00:00:00UTC

        #Start extract metadata
        minTime = datetime.strptime(data.time_coverage_start.strip(),'%Y-%m-%d %H:%M:%SUTC')
        maxTime = datetime.strptime(data.time_coverage_end.strip(),'%Y-%m-%d %H:%M:%SUTC')


        maxlat, minlat, maxlon, minlon = [float(data.geospatial_lat_max),
                                          float(data.geospatial_lat_min),
                                          float(data.geospatial_lon_max),
                                          float(data.geospatial_lon_min)]
        data.close()
        return minTime, maxTime, minlat, maxlat, minlon, maxlon


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
