from ..src.extract_ascii_metadata import ExtractASCIIMetadata
import os
import pathlib
from datetime import datetime, timedelta

class ExtractP3metnavimpactsMetadata(ExtractASCIIMetadata):
    """
    A class to extract p3metnavimpacts 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path

        self.fileformat = 'ASCII-ict'

        with open(self.file_path,'r') as f:
             self.file_lines = f.readlines()
        f.close()
 
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        :return:
        """
        num_header_lines = int(self.file_lines[0].split(',')[0])
        lines = self.file_lines[num_header_lines:]
        minTime, maxTime, minlat, maxlat, minlon, maxlon = [datetime(2100,1,1),
                                                            datetime(1900,1,1),
                                                            90.0,-90.0,180.0,-180.0]
        year = int(self.file_path.split('/')[-1].split('_')[3][0:4])
        for i in range(len(lines)):
            tkn = lines[i].split(',')
            sec, doy, lat, lon = [int(tkn[0]), int(tkn[1]), float(tkn[2]), float(tkn[3])]
            dt = datetime(year,1,1) + timedelta(seconds=sec) + timedelta(days=doy-1)
            minTime = min(minTime, dt)
            maxTime = max(maxTime, dt)
            if lat != -9999.0 and lon != -9999.0:
                maxlat = max(maxlat, lat)
                minlat = min(minlat, lat)
                maxlon = max(maxlon, lon)
                minlon = min(minlon, lon)

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a GIF file
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    [self.NLat, self.SLat, self.ELon, self.WLon]]
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
        start_date = self.minTime.strftime(date_format)
        stop_date = self.maxTime.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, format='ASCII-ict', version='1', **kwargs):
        """
        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        data = dict()
        data['GranuleUR'] = granule_name = os.path.basename(self.file_path)
        start_date, stop_date = self.get_temporal()
        data['ShortName'] = ds_short_name
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry()
        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in geometry_list)
        data['checksum'] = self.get_checksum()
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['DataFormat'] = self.fileformat
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting p3metnavimpacts Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_MetNav_P3B_20200220_R0.ict"
    exnet = ExtractP3metnavimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
