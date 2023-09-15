from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy.ma as ma
from datetime import datetime, timedelta, date
from netCDF4 import Dataset

class ExtractSbuairmarimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract sbuairmarimpacts 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.fileformat = 'netCDF-4'

        # extracting time and space metadata from .nc file
        dataset = Dataset(file_path)
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max(dataset)
        dataset.close()

    def get_variables_min_max(self, nc):
        """
        :param nc: Dataset opened
        :param file_path: file path
        :return:
        """

        #nc.variables['time'], long_name: Seconds since 1970-01-01 00:00:00
        utc_sec = nc.variables['time'][:]
        reftime = datetime(1970,1,1)
        minsec, maxsec = [int(ma.min(utc_sec)),int(ma.max(utc_sec))]
        t0, t1 = [reftime + timedelta(seconds=minsec), reftime + timedelta(seconds=maxsec)]

        if t0.date() in [datetime(2020,1,18).date(), datetime(2020,1,19).date(), datetime(2020,2,13).date()]: # Cedar Beach 40.965N, -73.030E
           lat = 40.965
           lon = -73.030
        elif t0.date() == datetime(2022,1,17).date(): #Elmira airport  42.1742 N -76.8719 E
           lat = 42.1742
           lon = -76.8719
        elif t0.date() == datetime(2022,1,29).date(): #Smith Point 40.7339 N   -72.8615E
           lat = 40.7339
           lon = -72.8615
        elif t0.date() == datetime(2022,2,25).date(): #Fort Edward 43.246 N   -73.559 E
           lat = 43.246
           lon = -73.559
        elif t0.date() == datetime(2023,1,25).date(): #Fort Edward 43.245 N, -73.559 E
           lat = 43.245
           lon = -73.559
        elif t0.date() == datetime(2023,1,28).date(): #Montauk Point, 41.065 N, -71.8627 E
           lat = 41.065
           lon = -71.8627
        else: #For the other days, Stony Brook University South Parking Lot 40.897N, -73.127E
           lat = 40.897
           lon = -73.127

        minTime, maxTime = [t0, t1]
        maxlat, minlat, maxlon, minlon = [lat+0.01, lat-0.01, lon+0.01, lon-0.01]

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
