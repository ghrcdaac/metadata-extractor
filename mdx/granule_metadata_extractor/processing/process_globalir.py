from ..src.extract_binary_metadata import ExtractBinaryMetadata
from datetime import datetime, timedelta
import os
import shutil


class ExtractGlobalirMetadata(ExtractBinaryMetadata):
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

        self.north, self.south, self.east, self.west = [66., -61., 180., -180.]

        self.get_variables_min_max()
        if file_path.endswith('.nc'):
           self.format='netCDF-4'
        else: #binary format, filename ending with '-mrest'
           self.format='Binary'
           self.new_filename = self.rename_file(mv=False)

    def get_variables_min_max(self, variable_key=None):
        """
        self.file_path: either binary file or netCDF-4 file 
        """
        filename = self.file_path.split('/')[-1]

        if filename.endswith('-mrest'): #binary 
           #i.e., globalir raw file name of format yyyymmdd_HHMM.wrld-ir4km-mrest
           self.end_time = datetime.strptime(filename.split('.')[0], '%Y%m%d_%H%M')
           self.start_time = self.end_time - timedelta(minutes=15)
        else: #netCDF-4
           #i.e.,globsat_s202401100010_ir.nc
           self.start_time = datetime.strptime(filename.split('_')[1],'s%Y%m%d%H%M')
           self.end_time = self.start_time + timedelta(minutes=10)


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

    def get_temporal(self, date_format='%Y-%m-%dT%H:%M:%SZ'):
        """
        :param date_format IF specified the return type will be a string type
        :return:
        """
        start_date = self.start_time.strftime(date_format)
        stop_date = self.end_time.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, version='01', format='Binary'):
        """

        :param ds_short_name:
        :param version:
        :param format:
        :return:
        """
        start_date, stop_date = self.get_temporal(date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        if self.new_filename:
           data['GranuleUR'] = self.new_filename
           data['UpdatedGranuleUR'] = self.new_filename
        else: #netCDF-4
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
    print('Extracting globalir Metadata')
