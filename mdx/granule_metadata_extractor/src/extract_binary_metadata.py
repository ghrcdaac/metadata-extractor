from .metadata_extractor import MetadataExtractor


class ExtractBinaryMetadata(MetadataExtractor):
    """
    Class to extract Binary metadata
    """
    def __init__(self, file_path):
        super().__init__(file_path)
        self.format = 'Binary'
    def get_variables_min_max(self, variable_key):
        """
        Abstract
        """
        pass
    def get_wnes_geometry(self):
        """
        Extract the geometry from a binary file
        :return: list of bounding box coordinates [west, north, east, south]
        """

        return [self.convert_360_to_180(self.west), self.north, self.convert_360_to_180(self.east), self.south]

    def get_temporal(self, date_format= '%Y-%m-%dT%H:%M:%SZ'):
        pass

    def get_metadata(self, ds_short_name, version, format):
        pass


if __name__ == '__main__':
    print("Dependencies works")

    file_path = "/home/amarouane/Downloads/GOESR_CRS_L1B_20170411_v0.nc"
    exnet = ExtractBinaryMetadata(file_path)
    # # print(exnet.get_checksum(file_path))
    # # print(exnet.get_file_size(file_path))
    # target_variable = 'time'
    # units_variable = 'units'
    # # uni = exnet.get_units(nc, target_variable)
    # # print(uni)
    # #
    # #
    # start, stop = exnet.get_temporal(units_variable=units_variable)
    # print(start, stop)

    target_variable_lat1 = 'lat'
    target_variable_lon1 = 'lon'
    K = exnet.get_wnes_geometry(target_variable_lon1, target_variable_lat1)

    print(K)

    #file_path = "local/HAMSR_L1B_20130925T051206_20130925T205025_v01.nc"
    #exnet = ExtractNetCDFMetadata(file_path)
    # print(exnet.get_checksum(file_path))
    # print(exnet.get_file_size(file_path))

    # uni = exnet.get_units(nc, target_variable)
    # print(uni)
    #
    #
    # start, stop = exnet.get_temporal()
    # print(start, stop)
    #
    # # target_variable_lat1 = {'units': 'degrees_north'}
    # # target_variable_lon1 = {'units': 'degrees_east'}
    # K = exnet.get_wnes_geometry()
    # #
    # print(K)
