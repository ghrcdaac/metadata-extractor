from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset
from pyhdf.HDF import *
from pyhdf.VS import *


class ExtractIsslisv1Metadata(ExtractNetCDFMetadata):
    """
    A class to extract both isslis_v1_nqc and isslis_v1_nrt 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        if file_path.endswith('.hdf'):
            self.fileformat = 'HDF-4'
        elif file_path.endswith('.nc'):
            self.fileformat = 'netCDF-4'

        # extracting time and space metadata from nc.gz file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max(file_path)

    def get_variables_min_max(self, filename):
        """
        :param nc: Dataset opened
        :param file_path: file path
        :return:
        """
        if filename.endswith('.nc'):
            return self.get_nc_metadata(filename)
        elif filename.endswith('.hdf'):
            return self.get_hdf_metadata(filename)

    def get_nc_metadata(self, filename):
        nc = Dataset(filename, 'r')
        timevar = nc.variables['one_second_TAI93_time'][:]
        lat = nc.variables['viewtime_lat'][:]
        lon = nc.variables['viewtime_lon'][:]
        nc.close()

        maxlat, minlat, maxlon, minlon = [max(lat), min(lat), max(lon), min(lon)]
        minTime, maxTime = [datetime(1993, 1, 1) + timedelta(seconds=min(timevar)),
                            datetime(1993, 1, 1) + timedelta(seconds=max(timevar))]

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_hdf_metadata(self, filename):
        f = HDF(filename)
        vs = f.vstart()
        # from one_second vdata to extract TAI93_time data field
        vd = vs.attach('one_second')
        # get all records
        recs = vd[:]
        # extract TAI93_time which is the first field
        timevar = [recs[i][0] for i in range(len(recs))]
        minTime, maxTime = [datetime(1993, 1, 1) + timedelta(seconds=min(timevar)),
                            datetime(1993, 1, 1) + timedelta(seconds=max(timevar))]
        vd.detach()
        # extract location info from viewtime vgroup first field (location)
        vd = vs.attach('viewtime')
        recs = vd[:]
        lat = [recs[i][0][0] for i in range(len(recs))]
        lon = [recs[i][0][1] for i in range(len(recs))]
        vd.detach()
        vs.end()
        f.close()
        maxlat, minlat, maxlon, minlon = [max(lat), min(lat),
                                          max(lon), min(lon)]

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


if __name__ == '__main__':
    print('Extracting isslis_v1_nqc or isslis_v1_nrt Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_CPL_L2_V1-02_01kmLay_20200115.hdf5"
    exnet = ExtractIsslisv1Metadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)