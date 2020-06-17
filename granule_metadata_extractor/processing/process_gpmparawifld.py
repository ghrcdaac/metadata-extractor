from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime
from zipfile import ZipFile


class ExtractGpmparawifldMetadata(ExtractASCIIMetadata):
    """
    A class to extract Gpmparawifld
    """
    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = -90.0
    south = 90.0
    East = -180.0
    West = 180.0

    def __init__(self, file_path):
        super().__init__(file_path)

        archive = ZipFile(file_path, 'r')
        files = archive.namelist()
        for items in files:
            self.file_lines = archive.open(items)
            self.get_variables_min_max(items)
            archive.close()

    def get_variables_min_max(self, file_name):
        """

        :param file_name: file name
        :param variable_key: The ASCII key we need to target
        :return: list longitude coordinates
        """
        fname = file_name.split('/')[-1]
        tkn0 = fname.split('_')

        lat = float(tkn0[3][1:]) / 10000.
        if 'S' in tkn0[3]:
            lat = -1. * lat
        lon = float(tkn0[4][1:]) / 10000.
        if 'W' in tkn0[4]:
            lon = -1. * lon

        for item in enumerate(self.file_lines):
            tkn = str(item[1]).split(';')
            tkn = tkn[0].split("b'")[-1]
            dt = datetime.strptime(tkn, '%Y%m%d%H%M%S')
            if self.start_time > dt:
                self.start_time = dt
            if self.end_time < dt:
                self.end_time = dt

        self.north, self.south = lat, lat
        self.east, self.west = lon, lon

        return

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a netCDF file
        :param nc_data: netCDF data
        :param timestamp:  The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in [self.north, self.south, self.east, self.west]]
        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]

    def get_temporal(self, time_variable_key='time', units_variable='units',  scale_factor=1.0, offset=0,
                     date_format = '%Y-%m-%dT%H:%M:%SZ'):
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

    def get_metadata(self, ds_short_name, format='ASCII', version='01'):
        """

        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        start_date, stop_date= self.get_temporal(time_variable_key='lon',
                                                                              units_variable='time',
                                                                              date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        gemetry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data

if __name__ == '__main__':
    print('Extracting Gpmodmlpvex Metadata')
    file_path = "/Users/navaneeth/dc8lase/granule-metadata-extractor/test/fixtures/jw_lpvex_Jarvenpaa_20101103-0000.txt"
    exnet = ExtractGpmparawifldMetadata(file_path)
    metada = exnet.get_metadata("test")
