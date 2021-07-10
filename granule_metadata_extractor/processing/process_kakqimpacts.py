from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import json
import os
import pathlib


class ExtractKakqimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract dataset metadata
    """

    def __init__(self, file_path):
        self.file_path = file_path
        self.granule_name = os.path.basename(file_path)
        with open(os.path.join(pathlib.Path(__file__).parent.absolute(),
                               '../src/helpers/kakqimpactsRefData.json'), 'r') as fp:
            #self.gpmseafluxicepop_loc = json.load(fp)
            self.dataset_loc = json.load(fp)

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
        return [float(x) for x in self.dataset_loc.get(self.granule_name).get('wnes_geometry')]

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
        start_date, stop_date = self.dataset_loc.get(self.granule_name).get('temporal')
        return start_date, stop_date

    def get_temporal_lookup(self, granule_name):
        """
        Because the files are huge we need to create a lookup file to get temporal
        instead of downloading the file an extracting it programaticlly
        Note that we already have get_metadata function that does that
        :param granule_name: filename also is used as a lookup key
        :return: start_date, stop_date
        """
        start_date, stop_date = self.dataset_loc.get(granule_name).get('temporal')
        return start_date, stop_date

    def get_wnes_geometry_lookup(self, granule_name):
        """
        Because the files are huge we need to create a lookup file to get geometry
        instead of downloading the file an extracting it programaticlly
        Note that we already have get_wnes_geometry function that does that
        :param granule_name: filename also is used as a lookup key
        :return: [west, north,east, south]
        """
        return self.dataset_loc.get(granule_name).get('wnes_geometry')

    def get_checksum(self):
        """
        Read checksum from ref file
        :return: checksum value as str
        """
        return self.dataset_loc.get(self.granule_name).get('checksum',
                                                           "09f7e02f1290be211da707a266f153b3")

    def get_file_size_megabytes(self):
        """
        Read file size for ref file
        :return: size in megabytes
        """
        return float(self.dataset_loc.get(self.granule_name).get('SizeMBDataGranule', "1400"))

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
        start_date, stop_date = self.get_temporal_lookup(granule_name)
        data['ShortName'] = ds_short_name
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry_lookup(granule_name)
        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in geometry_list)
        data['checksum'] = self.get_checksum()
        data['SizeMBDataGranule'] = str(self.get_file_size_megabytes())
        data['DataFormat'] = self.dataset_loc.get(granule_name).get('format')
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting kakqimpacts Metadata')
    #path_to_file = r"C:\Users\xli\xli\GHRC_cloud\tmp\seafluxicepop\SeaFluxV3_ICEPOP_025x025.nc4.gz"
#"IMPACTS_nexrad_20200221_200750_kakq.nc": {"temporal": ["2020-02-21T20:07:50Z", "2020-02-21T20:13:14Z"], "wnes_geometry": ["-82.181", "41.114", "-71.834", "32.854"], "SizeMBDataGranule": "2.27", "checksum": "2ecc7ae1b737eea6230221217092eef1", "format": "netCDF-4"}
    #path_to_file = "/ftp/ops/public/pub/fieldCampaigns/impacts/NEXRAD/KAKQ/data/20200106/IMPACTS_nexrad_20200106_005027_kakq.nc"
    path_to_file = "/ftp/ops/public/pub/fieldCampaigns/impacts/NEXRAD/KAKQ/data/20200221/IMPACTS_nexrad_20200221_200750_kakq.nc"

    exnet = ExtractKakqimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
