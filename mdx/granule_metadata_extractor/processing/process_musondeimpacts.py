from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime, timedelta
import numpy as np
import os

class ExtractMusondeimpactsMetadata(ExtractASCIIMetadata):
    """
    A class to extract musondeimpacts 
    """

    def __init__(self, file_path):
        self.file_path = file_path
        # these are needed to metadata extractor
        self.fileformat = 'ASCII'

        self.utf8_list = ['IMPACTS_upperair_UMILL_radiosonde_202201291800_QCMiller.txt',
                          'IMPACTS_upperair_UMILL_radiosonde_202201292000_QCMiller.txt',
                          'IMPACTS_upperair_UMILL_radiosonde_202201292200_QCMiller.txt',
                          'IMPACTS_upperair_UMILL_radiosonde_202202191800_QC.txt',
                          'IMPACTS_upperair_UMILL_radiosonde_202202191500_QC.txt']

        #extracting time and space metadata for ascii file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
            self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        :return:
        """

        fn = self.file_path.split('/')[-1]
        if '_windsonde1_' in fn: #wind sonde file
           #sample file:
           #IMPACTS_upperair_UMILL_windsonde1_202201162100_QCTeare.txt
           with open(self.file_path,'r') as f:
                lines = f.readlines()
           for line in lines:
               line = line.strip() # remove all the leading and trailing spaces from a string
               if line.startswith('XXX '):
                  start_time_str = '20'+line.split()[-1] #i.e., 220116/1958
                  minTime = datetime.strptime(start_time_str,'%Y%m%d/%H%M')
               elif line.startswith('Site'):
                  tkn = line.split()
                  lat0 = float(tkn[1].split(',')[0].split('=')[-1])
                  lon0 = float(tkn[2].split('=')[-1])
                  maxlat, minlat, maxlon, minlon = [lat0+0.01,
                                                    lat0-0.01,
                                                    lon0+0.01,
                                                    lon0-0.01]
               elif line.startswith('Saved by user: '):
                  maxTime = datetime.strptime(line,'Saved by user: User on %Y%m%d/%H%M UTC')
                  break
        else: #radio sonde file, either utf-8 or utf-16-be (big endian)
           with open(self.file_path,'rb') as f:
               lines = f.readlines()
           count = 0  #account number of header lines for later use
           for line in lines:
               count = count + 1
               if fn in self.utf8_list:
                  line = line.decode('utf-8',errors='ignore').strip()
               else: #utf-16-be
                  line = line.decode('utf-16-be',errors='ignore').strip()
               if line.startswith('Balloon release date and time'):
                  minTime = datetime.strptime(line.split()[-1].strip(),'%Y-%m-%dT%H:%M:%S') #i.e.,2022-01-29T13:07:23
               elif 'n Elapsed time  TimeUTC' in line:
                  num_header_lines = count + 1
                  break
           elap_sec = []
           lat = []
           lon = []
           for line in lines[num_header_lines:]:
               if fn in self.utf8_list:
                  line = line.decode('utf-8',errors='ignore').strip()
               else: #utf-16-be
                  line = line.decode('utf-16-be',errors='ignore').strip()
               if len(line) < 20 or 'row' in line:
                  continue
               tkn = line.split()
               elap_sec.append(float(tkn[1]))
               lat.append(float(tkn[-2]))
               lon.append(float(tkn[-1]))
           maxTime = minTime + timedelta(seconds = max(elap_sec))
           maxlat, minlat, maxlon, minlon = [max(lat),min(lat),max(lon),min(lon)]

        return minTime, maxTime, minlat, maxlat, minlon, maxlon


    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry
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
