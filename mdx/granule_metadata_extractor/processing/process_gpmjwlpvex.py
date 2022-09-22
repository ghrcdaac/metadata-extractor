from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime


file_lines = []
max_lat = 60.485+0.01
min_lat = 60.485-0.01
max_lon = 25.082+0.01
min_lon = 25.082-0.01
minTime = datetime.strptime('2100-01-0100:00:00', '%Y-%m-%d%H:%M:%S')
maxTime = datetime.strptime('1900-01-0100:00:00', '%Y-%m-%d%H:%M:%S')


class ExtractGpmjwlpvexMetadata(ExtractASCIIMetadata):
    """
    A class to extract Gpmodmlpvex
    """

    def __init__(self, file_path):
        global file_lines
        super().__init__(file_path)

        with open(file_path, 'r') as f:
            file_lines = f.readlines()

    def get_variables_min_max(self):
        """

        :param c_data: netCDF data
        :param variable_key: The NetCDF key we need to target
        :return: list longitude coordinates
        """
        global file_lines, minTime, maxTime
        for ii in range(1, len(file_lines)):
            tmp = file_lines[ii].split()

            dt = datetime.strptime(tmp[0] + tmp[1], "%Y-%m-%d%H:%M:%S")

            if dt < minTime:
                minTime = dt
            if dt > maxTime:
                maxTime = dt
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
        global max_lat, min_lat, max_lon, min_lon
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in [max_lat, min_lat, max_lon, min_lon]]
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
        global file_lines, minTime, maxTime
        self.get_variables_min_max()
        start_date = minTime.strftime(date_format)
        stop_date = maxTime.strftime(date_format)
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
    exnet = ExtractGpmjwlpvexMetadata(file_path)
    metada = exnet.get_metadata("test")
    # with open('the_filename.txt', 'rb') as f:
    #     my_list = pickle.load(f)
    #
    # for itm in my_list:
    #     print(exnet.get_wnes_geometry(itm))