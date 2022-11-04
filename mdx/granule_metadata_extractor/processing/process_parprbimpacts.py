from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
from datetime import datetime, timedelta
from netCDF4 import Dataset
import numpy as np
import os, pathlib, json

class ExtractParprbimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract Parprbimpacts netCDF-4
    """
    start_time = None
    end_time = None
    north = None
    east = None
    south = None
    west = None
    format = None

    def __init__(self, file_path):
        # super().__init__(file_path)
        self.file_path = file_path

        if file_path.endswith('.nc'):
           self.format = 'netCDF-4'
           self.get_variables_min_max_nc()

        else:
           self.format = 'PNG'
           with open(os.path.join(pathlib.Path(__file__).parent.absolute(),
                               '../src/helpers/parprbimpactsRefData.json'), 'r') as fp:
                self.dataset_loc = json.load(fp)
           self.get_variables_min_max_png()

    def get_variables_min_max_nc(self):
        """
        Extract metadata information from netcdf file
        """
        datafile = Dataset(self.file_path)
        lats = np.array(datafile['LAT'][:])
        lons = np.array(datafile['LON'][:])
        sec = np.array(datafile['time'][:])
        ref_time = datetime.strptime(datafile.FlightDate,'%m/%d/%Y')

        self.start_time, self.end_time = [ref_time+timedelta(seconds=sec.min()),
                                          ref_time+timedelta(seconds=sec.max())]
        self.north, self.south, self.east, self.west = [np.nanmax(lats),
                                                        np.nanmin(lats),
                                                        np.nanmax(lons),
                                                        np.nanmin(lons)]

    def get_variables_min_max_png(self):
        """
        Extract PNG metadata information from look up table 
        """
        key = self.file_path.split('/')[-1].split('_')[2] #i.e., 20200108
        self.start_time, self.end_time = [datetime.strptime(self.dataset_loc[key]['start'],'%Y-%m-%dT%H:%M:%SZ'),
                                          datetime.strptime(self.dataset_loc[key]['end'],'%Y-%m-%dT%H:%M:%SZ')]
        self.north, self.south, self.east, self.west = [self.dataset_loc[key]['NLat'],
                                                        self.dataset_loc[key]['SLat'],
                                                        self.dataset_loc[key]['ELon'],
                                                        self.dataset_loc[key]['WLon']]


    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
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

    def get_metadata(self, ds_short_name, time_variable_key='time', lon_variable_key='lon',
                     lat_variable_key='lat', time_units='units', format='netCDF-4', version='01'):
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
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        gemetry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = self.format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting parprbimpacts Metadata')
