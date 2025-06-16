from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import json
import os
import pathlib
from datetime import datetime, timedelta

class ExtractSportlisMetadata(ExtractNetCDFMetadata):
    """
    A class to extract sportlis
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        with open(os.path.join(os.path.dirname(__file__), f"../src/helpers/sportlis_county_bounding_box.json"), 'r') as fp:
            self.county_latlon = json.load(fp)

        self.fileformat = 'netCDF-4'

        # extracting time and space metadata from nc file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):
        #assign bounding box
        north, south, east, west = [52.915,25.075,-67.085,-124.925]
        #extract time info from file name
        filename = self.file_path.split('/')[-1]
        if filename.endswith('.dat'): #sportlis_SM_0_40cm_CLIMO_1981_2013_2013364.dat
           self.fileformat = "Binary"
           start_time = datetime.strptime(filename.split('.dat')[0].split('_')[-1],'%Y%j')
           end_time = start_time + timedelta(seconds=86399)
        elif filename.endswith('.out'): #sportlis_Yuma_County_CO_percentileSoil_1230.out
           self.fileformat = "ASCII"
           countyName = '_'.join(filename.split('_')[1:-2]) #i.e., Yuma_County_CO

           utc_date = filename.split('_')[-1].split('.out')[0] #i.e., 1230
           north = self.county_latlon[countyName]['north']
           south = self.county_latlon[countyName]['south']
           east = self.county_latlon[countyName]['east']
           west = self.county_latlon[countyName]['west']
           start_time = datetime.strptime('1981'+utc_date,'%Y%m%d')
           end_time = datetime.strptime('2013'+utc_date+'235959','%Y%m%d%H%M%S')
        elif filename.endswith('.grb2'):
           self.fileformat = "GRIB2"
           utc_str = filename.split('.grb2')[0].split('_')[-1]
           if len(utc_str) == 10: #i.e., sportlis_percentile_2023123112.grb2
              start_time = datetime.strptime(utc_str,'%Y%m%d%H')
           else: #i.e., sportlis_vsm_percentile_20231230.grb2
              start_time = datetime.strptime(utc_str,'%Y%m%d')
           end_time = start_time
        elif filename.endswith('grb'):#i.e., sportlis_HIST_202212300300_d01.grb
           self.fileformat = "GRIB1"
           utc_str = filename.split('_')[-2]
           start_time = datetime.strptime(utc_str,'%Y%m%d%H%M')
           end_time = start_time

        elif filename.endswith('.nc'): #i.e., sportlis_RST_NOAH33_202212010000_d01.nc
           self.fileformat = "netCDF-4"
           utc_str = filename.split('_')[-2] #i.e., 202212010000 202212010000
           start_time = datetime.strptime(utc_str,'%Y%m%d%H%M')
           end_time = start_time + timedelta(seconds = 86399)

        return start_time, end_time, south, north, west, east 

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
