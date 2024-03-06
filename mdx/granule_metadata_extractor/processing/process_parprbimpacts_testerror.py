from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import json
import os
import pathlib
from zipfile import ZipFile
from datetime import datetime

class ExtractParprbimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract parprbimpacts metadata
    """

    def __init__(self, file_path):
        self.file_path = file_path

        #Get lookup dataset's metadata attributes from lookup zip
        lookup_zip_path = os.path.join(pathlib.Path(__file__).parent.absolute(),
                               f"../src/helpers/parprbimpacts.zip")
        with ZipFile(lookup_zip_path) as lookup_zip:
            with lookup_zip.open("lookup.json") as collection_lookup:
                self.lookup_json = json.load(collection_lookup)

        if self.file_path.endswith('.nc'):#netCDF-4
           self.fileformat = 'netCDF-4'
           # extracting time and space metadata for netCDF-4 file
           [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
               self.get_variables_min_max_netcdf()
        else:#images
           self.fileformat = 'PNG'
           # extracting time and space metadata for PNG file
           [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
               self.get_variables_min_max_png()


    def get_variables_min_max_netcdf(self):
        """
        :return:
        """
        """
        Extract temporal and spatial metadata from netCDF files
        """
        filename = self.file_path.split('/')[-1]
        metadata = self.lookup_json[filename]
        #Example of metadata:
        #{"IMPACTS_2DSH-P3_20200118_sizedistributions_v01.nc":
        #  {"start": "2020-01-18T18:00:00Z",
        #   "end": "2020-01-19T00:02:00Z",
        #   "north": "44.202",
        #   "south": "37.914",
        #   "east": "-73.132",
        #   "west": "-75.518",
        #   "format": "netCDF-4",
        #   "sizeMB": 8.9
        #  }
        minTime = datetime.strptime(metadata['start'],'%Y-%m-%dT%H:%M:%SZ')
        maxTime = datetime.strptime(metadata['end'],'%Y-%m-%dT%H:%M:%SZ')
        minlat = float(metadata['south'])
        maxlat = float(metadata['north'])
        minlon = float(metadata['west'])
        maxlon = float(metadata['east'])
        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_variables_min_max_png(self):
        """
        :return:
        """
        utc_date = self.file_path.split('/')[-1].split('_')[-3] #i.e., 20200118
        keys = [x for x in self.lookup_json.keys() if utc_date in x]
        start = [datetime.strptime(self.lookup_json[x]['start'],'%Y-%m-%dT%H:%M:%SZ') for x in keys]
        end = [datetime.strptime(self.lookup_json[x]['end'],'%Y-%m-%dT%H:%M:%SZ') for x in keys]
        north = [float(self.lookup_json[x]['north']) for x in keys]
        south = [float(self.lookup_json[x]['south']) for x in keys]
        west = [float(self.lookup_json[x]['west']) for x in keys]
        east = [float(self.lookup_json[x]['east']) for x in keys]
        minTime = min(start)
        maxTime = max(end)
        minlat = min(south)
        maxlat = max(north)
        minlon = min(west)
        maxlon = max(east)

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
        #north, south, east, west = [round((x * 1.0) + 0, 3) for x in
        #                            [self.NLat, self.SLat, self.ELon, self.WLon]]
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

    def get_metadata(self, ds_short_name, format='netCDF-4', version='1', **kwargs):
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
    print('Extracting sbusndimpacts  Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_sounding_20200119_004158_SBU_Mobile.nc"
    exnet = ExtractSbusndimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
