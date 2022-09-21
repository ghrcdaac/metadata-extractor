from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
from datetime import datetime, timedelta
import gzip
import tempfile
import shutil
import os
try:
    import pyart
except ImportError:
    pyart = None


class ExtractNpolimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract Npolimpacts
    """
    start_time = None
    end_time = None
    north = None
    east = None
    south = None
    west = None

    def __init__(self, file_path):
        # super().__init__(file_path)
        self.file_path = file_path
        self.extract_metadata()

    def extract_metadata(self):
        """
        Extracts temporal and spatial metadata from npolimpacts files
        """

        with gzip.open(self.file_path, 'rb') as gz:
            tmp = tempfile.NamedTemporaryFile(delete=False)
            shutil.copyfileobj(gz, tmp)
            tmp.close()

        radar = pyart.io.read_cfradial(tmp.name)

        os.unlink(tmp.name)

        lat, lon = [radar.gate_latitude['data'][:], radar.gate_longitude['data'][:]]
        sec = radar.time['data'][:]

        dt0 = datetime.strptime(radar.time['units'].split(' ')[-1], '%Y-%m-%dT%H:%M:%SZ')
        self.start_time = dt0 + timedelta(seconds=int(min(sec)))
        self.end_time = dt0 + timedelta(seconds=int(max(sec)))

        self.north, self.south, self.east, self.west = [lat.max(), lat.min(), lon.max(), lon.min()]

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
                     lat_variable_key='lat', time_units='units', format='netCDF-3/CF', version='01'
                     ):
        """

        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :param version:
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
        data['DataFormat'] = 'netCDF-3/CF'
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Npolimpacts Metadata')
