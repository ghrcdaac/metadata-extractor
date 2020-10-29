from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
from datetime import datetime, timedelta
import os
import shutil

class ExtractGlobalirMetadata(ExtractNetCDFMetadata):
    """
    A class to extract metadata from globalir McIDAS binary files 
    """
    start_time = None
    end_time = None
    north = None 
    east = None 
    south = None 
    west = None 
    format = None
    new_filename = None

    def __init__(self, file_path):
        # super().__init__(file_path)
        self.file_path = file_path
        self.format = 'Binary'
        self.north, self.south, self.east, self.west = [66., -61., 180., -180.]

        self.get_variables_min_max()
        self.new_filename = self.rename_file(mv=False)

    def get_variables_min_max(self):
        """
        #self.file_path: globalir raw file name of format yyyymmdd_HHMM.wrld-ir4km-mrest
        """
        filename = self.file_path.split('/')[-1]
        self.end_time = datetime.strptime(filename.split('.')[0],'%Y%m%d_%H%M')
        self.start_time = self.end_time - timedelta(minutes=15)

    def rename_file(self, mv):
        """
        #Rename and create file
        :param self.file_path: globir raw file
        :param mv: rename the file on place (if False a copy will be used)
        :return: new file name
        """
        tkn = self.file_path.split('/')
        filename = tkn[-1]
        stopdatetime = datetime.strptime(filename.split('.')[0],'%Y%m%d_%H%M')
        number_of_days = stopdatetime.timetuple().tm_yday
        new_filename = "globir.%s%03d.%s" % (stopdatetime.strftime("%y"), number_of_days, stopdatetime.strftime("%H%M"))
        tkn[-1] = new_filename
        new_file_path = '/'.join(tkn)
        if mv: #rename
           os.rename(self.file_path,new_file_path)
        else: #copy
           shutil.copy(self.file_path,new_file_path)

        return new_filename


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
        #data['GranuleUR'] = self.file_path.split('/')[-1]
        data['GranuleUR'] = self.new_filename
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
    print('Extracting globalir Metadata')
