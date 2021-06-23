from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime
import numbers
import xlrd


class ExtractGpmarsifldMetadata(ExtractASCIIMetadata):
    """
    A class to extract gpmarsifld
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
        #from dataset user's guide
        self.loc = {'SF01': [42.542620, -93.589060],
                    'SF02': [42.469300, -93.565450],
                    'SF03': [42.452960, -93.567970],
                    'SF04': [42.544590, -93.525270],
                    'SF05': [42.428570, -93.521580],
                    'SF06': [42.389600, -93.500130],
                    'SF07': [42.515010, -93.472710],
                    'SF08': [42.484630, -93.441490],
                    'SF09': [42.445560, -93.444050],
                    'SF10': [42.379370, -93.402930],
                    'SF11': [42.429750, -93.365960],
                    'SF12': [42.341400, -93.334220],
                    'SF13': [42.403180, -93.309710],
                    'SF14': [42.328310, -93.254860],
                    'SF15': [42.420340, -93.220770] }
        self.get_variables_min_max(file_path)

    def get_variables_min_max(self, file_path):
        """
        Chooses which method to use to extract metadata info of file
        :return:
        :rtype:
        """
        if '.csv' in file_path:
            self.format = 'ASCII-csv'
            self.extract_csv_metadata()
        else:
            self.format = 'MS Excel'
            self.extract_excel_metadata()

    def extract_csv_metadata(self):
        """
        Extract metadata information from csv file
        """
        # read data to buffer
        with open(self.file_path,'rb') as fp:
             lines = fp.readlines()
             # skip first 4 header liens
             lines = lines[4:]

             self.start_time = datetime(2100, 1, 1)
             self.end_time = datetime(1900, 1, 1)
             site = ''
             for line in lines:
                 tkn = line.decode('utf-8').split(',')
                 if len(tkn) > 5:
                    dtstr = tkn[0]
                    if len(site) == 0:
                       site = tkn[2]
                    try:
                       dt = datetime.strptime(dtstr, '%Y/%m/%d %H:%M:%S')
                       self.start_time = min(dt, self.start_time)
                       self.end_time = max(dt, self.end_time)
                    except:
                       continue

             self.north = self.loc[site][0] + 0.01
             self.south = self.loc[site][0] - 0.01
             self.east = self.loc[site][1] + 0.01
             self.west = self.loc[site][1] - 0.01

    def extract_excel_metadata(self):
        """
        Extracts metadata information of excel file
        """
        self.start_time = datetime(2100, 1, 1)
        self.end_time = datetime(1900, 1, 1)
        site = ''

        #-- open excel file using xlrd
        xl_workbook = xlrd.open_workbook(self.file_path)
        xl_sheet = xl_workbook.sheet_by_index(0)

        #-- column 0 of the sheet contains time in fractional day
        for row_idx in range(4, xl_sheet.nrows):
            dtval = xl_sheet.cell_value(row_idx, 0)
            csite = xl_sheet.cell_value(row_idx, 2)
            if isinstance(dtval, numbers.Number):
                t_dt = xlrd.xldate_as_tuple(dtval, xl_workbook.datemode)
                dt = datetime(t_dt[0], t_dt[1],t_dt[2],t_dt[3],t_dt[4],t_dt[5])
                self.start_time = min(dt, self.start_time)
                self.end_time = max(dt, self.end_time)
            if len(site) == 0 and len(csite) > 0:
                site = csite

        xl_workbook.release_resources()
        del xl_workbook

        self.north = self.loc[site][0] + 0.01
        self.south = self.loc[site][0] - 0.01
        self.east = self.loc[site][1] + 0.01
        self.west = self.loc[site][1] - 0.01

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a file
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

    def get_metadata(self, ds_short_name, time_units='units', format='ASCII-csv', version='01'):
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
    print('Extracting gpmarsifld Metadata')
