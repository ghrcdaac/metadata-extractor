from ..src.extract_browse_metadata import ExtractBrowseMetadata
import json
import os
import pathlib
# from os import path
from datetime import datetime

tcspecmwf_loc = {}
north = 0
south = 0
west = 0
east = 0
start_time = datetime(2100, 1, 1)
end_time = datetime(1900, 1, 1)


class ExtractTcspecmwfMetadata(ExtractBrowseMetadata):
    """
    A class to extract Tcspecmwf
    """

    def __init__(self, file_path):
        global tcspecmwf_loc, north, south, east, west, start_time, end_time
        super().__init__(file_path)

        with open(os.path.join(pathlib.Path(__file__).parent.absolute(),
                               '../src/helpers/tscpecmwfRefData.json'), 'r') as fp:
            tcspecmwf_loc = json.load(fp)
        self.granule_name = os.path.basename(file_path)
        # file_name_list = tarfile.open(file_path).getnames()
        # default_val = tcspecmwf_loc.get('default')
        #
        # for key in file_name_list:
        #     north = tcspecmwf_loc.get(key, default_val).get("north")
        #     south = tcspecmwf_loc.get(key, default_val).get("south")
        #     east = tcspecmwf_loc.get(key, default_val).get("east")
        #     west = tcspecmwf_loc.get(key, default_val).get("west")
        #     end_time = datetime.strptime(tcspecmwf_loc.get(key, default_val).get("end"),
        #                                  '%Y%m%dT%HZ')
        #     start_time = datetime.strptime(tcspecmwf_loc.get(key, default_val).get("start"),
        #                                    '%Y%m%dT%HZ')

    def get_variables_min_max(self, data):
        """

        :param data:
        :return:
        """
        pass

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a GIF file
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        # global north, south, east, west
        # north, south, east, west = [round((x * scale_factor) + offset, 3) for x in [north, south, east, west]]
        # return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]
        return [float(x) for x in tcspecmwf_loc.get(self.granule_name).get('wnes_geometry')]

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
        # global start_time, end_time
        # start_date = datetime.strptime(str(start_time), '%Y-%m-%d %H:%M:%S')
        # start_date = start_date.strftime(date_format)
        #
        # stop_date = datetime.strptime(str(end_time), '%Y-%m-%d %H:%M:%S')
        # stop_date = stop_date.strftime(date_format)
        #
        # return start_date, stop_date
        start_date, stop_date = tcspecmwf_loc.get(self.granule_name).get('temporal')
        return start_date, stop_date


    def get_temporal_lookup(self, granule_name):
        """
        Because the files are huge we need to create a lookup file to get temporal
        instead of downloading the file an extracting it programaticlly
        Note that we already have get_metadata function that does that
        :param granule_name: filename also is used as a lookup key
        :return: start_date, stop_date
        """

        start_date, stop_date = tcspecmwf_loc.get(granule_name).get('temporal')
        return start_date, stop_date


    def get_wnes_geometry_lookup(self, granule_name):
        """
        Because the files are huge we need to create a lookup file to get geometry
        instead of downloading the file an extracting it programaticlly
        Note that we already have get_wnes_geometry function that does that
        :param granule_name: filename also is used as a lookup key
        :return: [west, north,east, south]
        """
        return tcspecmwf_loc.get(granule_name).get('wnes_geometry')

    def get_checksum(self):
        """
        Read checksum from ref file
        :return: checksum value as str
        """
        return tcspecmwf_loc.get(self.granule_name).get('checksum',
                                                           "09f7e02f1290be211da707a266f153b3")

    def get_file_size_megabytes(self):
        """
        Read file size for ref file
        :return: size in megabytes
        """
        return float(tcspecmwf_loc.get(self.granule_name).get('SizeMBDataGranule', "1400"))

    def get_metadata(self, ds_short_name, format='GRIB', version='1'):
        """

        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        # start_date, stop_date = self.get_temporal(time_variable_key='lon', units_variable='time',
        #                                           date_format='%Y-%m-%dT%H:%M:%SZ')

        #data['GranuleUR'] = self.file_path.split('/')[-1]
        data = dict()
        data['GranuleUR'] = granule_name = os.path.basename(self.file_path)
        start_date, stop_date = self.get_temporal_lookup(granule_name)
        data['ShortName'] = ds_short_name
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        #geometry_list = self.get_wnes_geometry()
        geometry_list = self.get_wnes_geometry_lookup(granule_name)
        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(str(x) for x in geometry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting TCSPECMWF Metadata')
    path_to_file = "../test/fixtures/tcspecmwf_2005_180_daily.tar"
    exnet = ExtractTcspecmwfMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
    # with open('the_filename.txt', 'rb') as f:
    #     my_list = pickle.load(f)
    #
    # for itm in my_list:
    #     print(exnet.get_wnes_geometry(itm))
