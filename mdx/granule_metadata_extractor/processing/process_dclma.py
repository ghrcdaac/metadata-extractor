from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime, timedelta
import os
import re
import gzip

#Exclude all data located more than 200 km from DCLMA center point
north0, south0, east0, west0 = [40.688, 37.091, -74.724, -79.346]

class ExtractDclmaMetadata(ExtractASCIIMetadata):
    """
    A class to extract dclma granule metadata
    """
    #Exclude all data located more than 200 km from DCLMA center point
    #north0, south0, east0, west0 = [40.688, 37.091, -74.724, -79.346]

    def __init__(self, file_path):
        super().__init__(file_path)
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.get_variables_min_max()

    def get_variables_min_max(self, **kwargs):
        """
        Extracts temporal and spatial metadata from dclma granules
        """
        #Process *.dat.gz files
        #with gzip.GzipFile(self.file_path, mode='rb') as gzipfile:
        with gzip.open(self.file_path, 'r') as f:
             encoded_lines = f.readlines()

        lines = []
        for encoded_line in encoded_lines:
            line = encoded_line.decode("utf-8")
            line = line.replace('\n','')
            lines.append(line)
        tmp_str = [x for x in lines if 'Data start time:' in x]
        utc_date_str = tmp_str[0].split()[3] #i.e., 12/21/25
        utc_date = datetime.strptime(utc_date_str,'%m/%d/%y')

        tmp_str = [x for x in lines if '*** data ***' in x]
        index = lines.index(tmp_str[0])

        utc = []
        lats = []
        lons = []
        for line in lines[index+1:]:
            tkn = line.split()
            utc.append(float(tkn[0]))
            lats.append(float(tkn[1]))
            lons.append(float(tkn[2]))

        self.start_time = utc_date + timedelta(seconds = int(min(utc)))
        self.end_time = utc_date + timedelta(seconds = int(max(utc)))

        north, south, east, west = [max(lats),min(lats),max(lons),min(lons)]

        #Exclude all data located more than 200 km from DCLMA center point
        self.north = min(north0,north)
        self.south = max(south0,south)
        self.east = min(east0,east)
        self.west = max(west0,west)


    def get_wnes_geometry(self, scale_factor=1.0, offset=0, **kwargs):
        """
        Extract the geometry from a netCDF file
        :param nc_data: netCDF data
        :param timestamp:  The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    [self.north, self.south, self.east, self.west]]
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
        start_date = self.start_time.strftime(date_format)
        stop_date = self.end_time.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, format='Binary', version='01', **kwargs):
        """

        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        start_date, stop_date = self.get_temporal(time_variable_key='lon',
                                                  units_variable='time',
                                                  date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_name
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        gemetry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Nalma Metadata')
