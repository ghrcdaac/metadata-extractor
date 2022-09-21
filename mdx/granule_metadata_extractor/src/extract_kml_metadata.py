from .metadata_extractor import MetadataExtractor
from abc import abstractmethod


class ExtractKMLMetadata(MetadataExtractor):
    """
    Class to extract ASCII metadata
    """
    def __init__(self, file_path):
        super().__init__(file_path)

    @abstractmethod
    def get_variables_min_max(self, postion):
        """
        abstract
        """
        pass

    @abstractmethod
    def get_temporal(self, time_position=0, time_units='hours', scale_factor=1.0, offset=0,
                     date_format = '%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The NetCDF variable we need to target
        :param units_variable: The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """
        pass

    @abstractmethod
    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a netCDF file
        :param nc_data: netCDF data
        :param variable_lon_key:  The NetCDF variable we need to target
        :param variable_lat_key:  The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        pass

    @abstractmethod
    def get_metadata(self, ds_short_name, time_position=0, time_units="hours", lon_postion=15, date_format='%Y-%m-%dT%H:%M:%SZ',
                     lat_postion=16, format='kml', version='1'):
        """

        :param ds_short_name:
        :param time_position:
        :param time_units:
        :param lon_postion:
        :param date_format:
        :param lat_postion:
        :param format:
        :param version:
        :return:
        """
        pass
        return 0

if __name__ == '__main__':
    print("Dependencies works")

    file_path = "/home/amarouane/Downloads/lpvex_Kumpula_20101012_0834.tsv"
    exnet = ExtractKMLMetadata(file_path)
    metada = exnet.get_metadata("test")
    print(metada)
