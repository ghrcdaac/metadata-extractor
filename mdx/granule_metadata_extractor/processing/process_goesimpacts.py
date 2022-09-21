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



class ExtractGoesimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract goesimpacts 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path

        # extracting time and space metadata from nc.gz file
        dataset = Dataset(file_path)
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max(dataset, file_path)
        dataset.close()

    def get_variables_min_max(self, nc, file_path):
        """
        :param nc: Dataset opened
        :param file_path: file path
        :return:
        """
        if '_conus_' in file_path:
            minTime, maxTime, minlat, maxlat, minlon, maxlon = self.get_CONUS_metadata(nc)
        elif '_mesoscale_' in file_path:
            minTime, maxTime, minlat, maxlat, minlon, maxlon = self.get_meso_metadata(nc)       

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_meso_metadata(self, nc):
        # get bounding box
        geospatial_lat_lon_extent = nc.variables['geospatial_lat_lon_extent']
        maxlat, minlat, maxlon, minlon = \
            [ geospatial_lat_lon_extent.geospatial_northbound_latitude,
              geospatial_lat_lon_extent.geospatial_southbound_latitude,
              geospatial_lat_lon_extent.geospatial_eastbound_longitude,
              geospatial_lat_lon_extent.geospatial_westbound_longitude ]
        # get time range
        time_coverage_start = nc.getncattr('time_coverage_start')
        time_coverage_end = nc.getncattr('time_coverage_end')
        minTime, maxTime = [datetime.strptime(time_coverage_start, "%Y-%m-%dT%H:%M:%S.%fZ"),
                            datetime.strptime(time_coverage_end, "%Y-%m-%dT%H:%M:%S.%fZ")]

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_CONUS_metadata(self, nc):
        lat = nc.variables['y0'][:]
        lon = nc.variables['x0'][:]
        starttime = nc.variables['start_time'][:][0]
        endtime = nc.variables['stop_time'][:][0]
        minTime, maxTime = [datetime(1970, 1, 1) + timedelta(seconds=starttime),
                            datetime(1970, 1, 1) + timedelta(seconds=endtime)]
        maxlat, minlat, maxlon, minlon = [max(lat), min(lat), max(lon), min(lon)]

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
        data['DataFormat'] = 'netCDF-4'
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting goesimpacts  Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_goes16_conus_20200102_002856_ch08.nc"
    exnet = ExtractSbusndimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
