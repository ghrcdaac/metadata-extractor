from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime, timedelta
import os
import re
import gzip


class ExtractNalmaMetadata(ExtractASCIIMetadata):
    """
    A class to extract Nalma granule metadata
    """
    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = -90.0
    south = 90.0
    east = -180.0
    west = 180.0

    def __init__(self, file_path):
        super().__init__(file_path)
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.get_variables_min_max()

    def get_variables_min_max(self, **kwargs):
        """
        Extracts temporal and spatial metadata from nalma granules
        """
        #Sample: 
        #NALMA_201016_142000_0600.dat.gz 
        #LYLOUT_131231_220000_3600.dat.gz
        if self.file_name.startswith('NALMA') or self.file_name.startswith('LYLOUT'):
           tkn=re.search(r'^.*(\d{6}_\d{6}_\d{4}).*$',self.file_name)[1].split('_') #i.e., 201016,142000,0600
           secs = int(tkn[-1]) #i.e., 600 seconds
           self.start_time = min(datetime.strptime('_'.join(tkn[0:2]),'%y%m%d_%H%M%S'),
                                 self.start_time)
           self.end_time = max(self.end_time, self.start_time + timedelta(seconds=secs-1))
        
        elif self.file_name.startswith('LMA_NA'): #Sample: LMA_NA_6.1_080_2001-11-24_22-00-00.dat.gz
           tkn=re.search(r'^.*(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}).*$',self.file_name)[1] #i.e.,2001-11-24_22-00-00
           secs = 3600 #seconds
           self.start_time = min(datetime.strptime(tkn,'%Y-%m-%d_%H-%M-%S'),
                                 self.start_time)
           self.end_time = max(self.end_time, self.start_time + timedelta(seconds=secs-1))

        elif self.file_name.startswith('nalma_lylout'): #Sample:nalma_lylout_20170630_22_3600.dat.gz
           tkn=re.search(r'^.*(\d{8}_\d{2}_\d{4}).*$',self.file_name)[1].split('_') #i.e.,20170630,22,3600
           secs = int(tkn[-1]) #i.e., 3600 seconds
           self.start_time = min(datetime.strptime('_'.join(tkn[0:2]),'%Y%m%d_%H'),
                                 self.start_time)
           self.end_time = max(self.end_time, self.start_time + timedelta(seconds=secs-1))

        if self.file_name.endswith('.qua.gz'):
           self.north, self.south, self.east, self.west = [36.72461000, 32.72461000, -84.64533000,
                                                          -88.64533000]
        else: #*.dat.gz
           #Extract lat/lon from file lines:
           header_flag = False
           with gzip.open(self.file_path, 'r') as f:
               for line in f.readlines():
                   if header_flag:
                      self.north, self.south = [max(self.north, float(line.split()[1])),
                                                min(self.south, float(line.split()[1]))]
                      self.east, self.west = [max(self.east, float(line.split()[2])),
                                              min(self.west, float(line.split()[2]))]
                   if b'*** data ***' in line:
                      header_flag = True

           if self.north == self.south and self.east == self.west:
               self.north, self.south, self.east, self.west = [self.north + 0.001, self.south - 0.001,
                                                               self.east + 0.001, self.west - 0.001]

           #For *.dat.gz with no data
           if self.north == -90.0:
               self.north, self.south, self.east, self.west = [36.72461000, 32.72461000, -84.64533000,
                                                               -88.64533000]

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
