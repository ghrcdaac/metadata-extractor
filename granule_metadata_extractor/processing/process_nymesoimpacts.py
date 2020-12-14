from ..src.extract_browse_metadata import ExtractBrowseMetadata
import os
import numpy as np
from datetime import datetime, timedelta
import json

class ExtractNymesoimpactsMetadata(ExtractBrowseMetadata):
    """
    A class to extract nymesoimpacts 
    """

    def __init__(self, file_path):
        self.file_path = file_path
        # these are needed to metadata extractor
        self.fileformat = 'PNG'
        self.nysm_path = '../src/helpers/meta_nysm_latlon.csv'
        self.prof_path = '../src/helpers/meta_prof_latlon.csv'

        [self.nysm, self.prof] = self.get_site_lat_lon()

        # extracting time and space metadata for png file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
            self.get_variables_min_max()

    def get_site_lat_lon(self):
        """
        :return:
        """
        nysm = {}
        prof = {}
        #read meta_nysm_latlon.csv
        with open(os.path.join(os.path.dirname(__file__),self.nysm_path),'r') as fp:
             lines = fp.readlines()
        for i in range(2,len(lines)):
            #Sample line:
            #WBOU,94,Woodbourne,41.7451,-74.5883,432.999,Woodbourne,BGM
            if not lines[i].startswith(',,,'):
               tkn = lines[i].split(',')
               nysm[tkn[0].lower()] = {'lat':float(tkn[3]),'lon':float(tkn[4])}

        #read meta_prof_latlon.csv
        with open(os.path.join(os.path.dirname(__file__),self.prof_path),'r') as fp:
             lines = fp.readlines()
        for line in lines:
            if 'PROF_' in line:
               tkn = line.split(',')
               prof[tkn[0].split('_')[1].lower()] = {'lat':float(tkn[3]),'lon':float(tkn[4])}
        return nysm, prof

    def get_variables_min_max(self):
        """
        :return:
        """
        fn=self.file_path.split('/')[-1]
        #Sample file
        #IMPACTS_NYS_mwr_cloud_202001032300_redh.png
        tkn = fn.split('.')[0].split('_')
        minTime = datetime.strptime(tkn[-2],'%Y%m%d%H%M')
        maxTime = minTime + timedelta(hours=24)

        if 'mwr' in tkn or 'ground' in tkn: #MESONET ground observation
           if tkn[-1] in self.nysm.keys():
              stn_lat = self.nysm[tkn[-1]]['lat']
              stn_lon = self.nysm[tkn[-1]]['lon']
           else:
              stn_lat = self.prof[tkn[-1]]['lat']
              stn_lon = self.prof[tkn[-1]]['lon']
        elif 'lidar' in tkn:
           stn_lat = self.prof[tkn[-1]]['lat']
           stn_lon = self.prof[tkn[-1]]['lon']

        maxlat = stn_lat+0.01
        minlat = stn_lat-0.01
        maxlon = stn_lon+0.01
        minlon = stn_lon-0.01

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

    def get_metadata(self, ds_short_name, format='PNG', version='1', **kwargs):
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


if __name__ == '__main__':
    print('Extracting phipsimpacts Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_PHIPS_20200201_1109_20200201113854_000006_C2.png"
    exnet = ExtractSbuceilimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
