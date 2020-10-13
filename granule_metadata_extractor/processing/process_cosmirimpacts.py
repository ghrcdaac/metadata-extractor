from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset



class ExtractCosmirimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract cosmirimpacts 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.fileformat = 'netCDF-4'

        # extracting time and space metadata from nc.gz file
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

        lat = nc.variables['Latitude'][:]
        lon = nc.variables['Longitude'][:]
        year = nc.variables['Year'][:]
        mon = nc.variables['Month'][:]
        day = nc.variables['DayOfMonth'][:]
        hr = nc.variables['Hour'][:]
        mn = nc.variables['Minute'][:]
        sec = nc.variables['Second'][:]

        #set missing values in lat and lon to np.nan
        lat[lat==-999.0] = np.nan
        lon[lon==-999.0] = np.nan
        maxlat, minlat, maxlon, minlon = [np.nanmax(lat), np.nanmin(lat),
                                          np.nanmax(lon), np.nanmin(lon)]

        minTime, maxTime = [datetime(2100, 1, 1), datetime(1900, 1, 1)]
        for i in range(sec.shape[0]):
            for j in range(sec.shape[1]):
                if int(year[i,j]) != -999.0:
                    total_sec = int(hr[i,j]*3600 + mn[i,j]*60 + sec[i,j])
                    dt = datetime(int(year[i,j]), int(mon[i,j]), int(day[i,j])) + \
                         timedelta(seconds=total_sec)
                    minTime = min(dt, minTime)
                    maxTime = max(dt, maxTime)

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
    print('Extracting cosmrrimpacts  Metadata')
    path_to_file = "../../test/fixtures/impacts_cosmir_20200227_forward_conical_v1.nc"
    exnet = ExtractCosmirimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
