from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime


class ExtractNamdrop_rawMetadata(ExtractASCIIMetadata):
    """
    A class to extract Namdrop_raw
    """
    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = -90.0
    south = 90.0
    east = -180.0
    west = 180.0

    def __init__(self, file_path):
        super().__init__(file_path)

        file_name = file_path.split('/')[-1]

        with open(file_path, 'r') as f:
            self.file_lines = f.readlines()

        self.get_variables_min_max(file_name)

    @staticmethod
    def case_5(longitude, fname):
        """
        Test for exception case for namdrop_raw normal metadata extraction for
        file NAMMA_DROP_20060903_121730_P.dat
        :param longitude: longitude of granule line
        :param fname: granule name
        :return: boolean for whether the input is an exception or not
        """
        return longitude < -59. and fname == 'NAMMA_DROP_20060903_121730_P.dat'

    @staticmethod
    def case_6(longitude, fname):
        """
        Test for exception case for namdrop_raw normal metadata extraction for
        file NAMMA_DROP_20060908_171716_P.dat
        :param longitude: longitude of granule line
        :param fname: granule name
        :return: boolean for whether the input is an exception or not
        """
        return longitude < -42. and fname == 'NAMMA_DROP_20060908_171716_P.dat'

    def get_variables_min_max(self, file_name):
        """

        :param file_name: file name
        :param variable_key: The ASCII key we need to target
        :return: list longitude coordinates
        """

        type_str = ['STA', 'COM', 'LAU', 'VER', 'FMT', 'TOF', 'END']
        case_1 = [106.260960, -5.458448, "NAMMA_DROP_20060909_154501_P.dat"]
        case_2 = [73.524022, 3.685748, "NAMMA_DROP_20060904_145136_P.dat"]
        case_3 = [-15.039764, 28.944029, "NAMMA_DROP_20060904_140230_P.dat"]
        case_4 = [-15.037447, 28.943002, "NAMMA_DROP_20060904_140230_P.dat"]

        dt = []
        dt_err = []
        lat = []
        lon = []
        for line in self.file_lines:
            tkn = line.split()

            if not tkn:
                continue

            if tkn[1] not in type_str:
                if len(tkn) != 20:
                    continue

                dt_str = f"20{tkn[3]}{tkn[4]}"

                clon, clat = [float(tkn[11]), float(tkn[12])]

                # We found 6 data files containing incorrect lat/lon (sudden lat/lon jump)
                # Below we manually skip these wrong lat/lon values from these 5 files:
                if [clon, clat, file_name] in \
                        [case_1, case_2, case_3, case_4] or self.case_5(clon, file_name) or \
                        self.case_6(clon, file_name) or clon < -180 or clon > 180 or clat < -90 \
                        or clat > 90:

                    dt_err.append(datetime.strptime(dt_str, '%Y%m%d%H%M%S.%f'))
                else:
                    lon.append(clon)
                    lat.append(clat)
                    dt.append(datetime.strptime(dt_str, '%Y%m%d%H%M%S.%f'))

        if dt:
            self.start_time = min(dt)
            self.end_time = max(dt)
            self.north = max(lat)
            self.south = min(lat)
            self.east = max(lon)
            self.west = min(lon)
        else:
            self.start_time = min(dt_err)
            self.end_time = max(dt_err)
            self.north = 47.453333
            self.south = 7.48492
            self.east = -12.74101
            self.west = -93.799061

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
        data['DataFormat'] = format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Namdrop_raw Metadata')
    path_to_file = r'C:\Users\ecampos\Documents\granule-metadata-extractor\test\fixtures' \
                   r'\NAMMA_DROP_20060807_193132_P.dat'
    exnet = ExtractNamdrop_rawMetadata(path_to_file)
    metada = exnet.get_metadata("test")
