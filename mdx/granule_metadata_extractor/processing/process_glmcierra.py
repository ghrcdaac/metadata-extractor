from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset

class ExtractGlmcierraMetadata(ExtractNetCDFMetadata):
    """
    A class to extract glmcierra 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these 5 files below have incorrect lat/lon info
        #we assign [90,-90,180,-180] temporarily
        #after finishing in PROD, come back to assign summary metadata to them
        self.file_excluded = ['OR_GLM-L2-CIERRA-DB_GOES-EAST_s20192931845000.nc',
                              'OR_GLM-L2-CIERRA-DB_GOES-EAST_s20193132345000.nc',
                              'OR_GLM-L2-CIERRA-DB_GOES-WEST_s20203590215000.nc',
                              'OR_GLM-L2-CIERRA-DB_GOES-WEST_s20203591600000.nc',
                              'OR_GLM-L2-CIERRA-DB_GOES-WEST_s20210122000000.nc']
        self.fileformat = 'netCDF-4'

        # extracting time and space metadata from nc file
        dataset = Dataset(file_path)
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max(dataset, file_path)
        dataset.close()

    def get_variables_min_max(self, datafile, filename):
        """
        :param datafile: Dataset opened
        :param filename: file path
        :return:
        """
        ftype = datafile.file_format
        if ftype.startswith('NETCDF3'):
            file_type = "netCDF-3"
        else:
            file_type = "netCDF-4"

        lats = np.array(datafile['FLASH_LAT'][:])
        lons = np.array(datafile['FLASH_LON'][:])

        maxlat, minlat, maxlon, minlon = [np.nanmax(lats),
                                          np.nanmin(lats),
                                          np.nanmax(lons),
                                          np.nanmin(lons)]
        if filename.split('/')[-1] in self.file_excluded:
            #assign fake bounding box values; will fix in lookup.json later
            maxlat, minlat, maxlon, minlon = [90.,-90.,180.,-180.]

        minTime = datetime.strptime(datafile.TIME_COVERAGE_START,'%Y-%m-%d %H:%M:%SZ')
        maxTime = datetime.strptime(datafile.TIME_COVERAGE_END,'%Y-%m-%d %H:%M:%SZ')

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
    print('Extracting glmcierra  Metadata')
    path_to_file = "../../test/fixtures/"
    exnet = ExtractSbuceilimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
