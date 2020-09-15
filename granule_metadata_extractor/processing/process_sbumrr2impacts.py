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



class ExtractSbumrr2impactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract sbumrr2impacts 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.file_format = {'BNL':'netCDF-3','RT':'netCDF-4/CF',
                            'MAN':'netCDF-4/CF'}
        self.gloc = {'BNL':[40.879,-72.874],'MAN':[40.7282,-74.0068]} #lat,lon
        #RT default site: SBU (40.897N, -73.127E)
        #RT periods (if not within any of the three periods, it's at SBU site)
        self.RT_period = [{'Smith Point':[datetime(2019,12,13,16,53),
                           datetime(2019,12,13,19,53)]},
                          {'Cedar Beach':[datetime(2020,1,18,16,8),
                           datetime(2020,1,19,1,23)]},
                          {'Cedar Beach':[datetime(2020,2,13,2,38),
                           datetime(2020,2,13,11,8)]}]
        self.RT_loc = {'SBU':[40.897,-73.127],
                       'Smith Point':[40.733,-72.862],
                       'Cedar Beach':[40.965,-73.030]}

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
        #BNL, RT and MAN files
        site_id = file_path.split('/')[-1].split('_')[-1].split('.')[0] #BNL, RT, or MAN
        self.fileformat = self.file_format[site_id]
        if site_id == 'BNL':
            dt0 = datetime.strptime(nc.variables['time'].getncattr('units').split()[-1],'%Y-%m-%d')
        else: #RT and MAN
            dt0 = datetime.strptime(nc.variables['time'].getncattr('units').split()[-1],'%Y-%m-%dT%H:%M:%SZ')

        sec = nc.variables['time'][:] #seconds since 1970-1-1
        minTime = dt0 + timedelta(seconds=int(min(sec).item()))
        maxTime = dt0 + timedelta(seconds=int(max(sec).item()))

        if site_id == 'RT':
            site_lat, site_lon = self.get_RT_lat_lon(minTime, maxTime)
        else: #MAN or BNL
            site_lat = self.gloc[site_id][0]
            site_lon = self.gloc[site_id][1]

        maxlat, minlat, maxlon, minlon = [site_lat+0.01, site_lat-0.01, 
                                          site_lon+0.01, site_lon-0.01]


        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_RT_lat_lon(t0,t1):
        flag = 0
        for period in self.RT_period:
            if t0 >= period[list(period.keys())[0]][0] and t1 <= period[list(period.keys())[0]][1]:
                flag = 1 #found
                site_name = list(period.keys())[0] #'Smith Point' or 'Cedar Beach' 
                break

        if flag == 0:
            site_name = 'SBU'

        lat = self.RT_loc[site_name][0]
        lon = self.RT_loc[site_name][1]

        return lat,lon


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


if __name__ == '__main__':
    print('Extracting sbumrr2impacts  Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_SBU_mrr2_20200106_BNL.nc"
    exnet = ExtractSbumrr2impactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
