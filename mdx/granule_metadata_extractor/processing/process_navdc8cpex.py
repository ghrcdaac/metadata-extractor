from ..src.extract_ascii_metadata import ExtractASCIIMetadata
import os
import numpy as np
from datetime import datetime, timedelta

class ExtractNavdc8cpexMetadata(ExtractASCIIMetadata):
    """
    A class to extract navdc8cpex 
    """

    def __init__(self, file_path):
        self.file_path = file_path
        # these are needed to metadata extractor
        self.fileformat = 'ASCII'

        # extracting time and space metadata for ascii file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
            self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        :return:
        """
        with open(self.file_path,'r') as fp:
             lines = fp.readlines()
        num_header_lines = int(lines[0].split(',')[0])
        utc_time = []
        lat = []
        lon = []
        for line in lines[num_header_lines:]:
            #Start_UTC,Day_Of_Year,Latitude,Longitude,......
            #Time_Start: start time in seconds from midnight in UTC
            #Day_Of_Year: day of the year
            tkn = line.split(',')
            time_ref = ''.join(['2017',tkn[1].zfill(3)])
            utc = datetime.strptime(time_ref, '%Y%j') + timedelta(seconds=float(tkn[0]))
            utc_time.append(utc)
            lat.append(float(tkn[2]))
            lon.append(float(tkn[3]))

        minTime, maxTime = [min(utc_time), max(utc_time)]
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

    def get_metadata(self, ds_short_name, format='ASCII', version='1', **kwargs):
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
