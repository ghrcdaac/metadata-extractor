from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset



class ExtractCplimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract cplimpacts 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.fileformat = 'HDF-5'

        # extracting time and space metadata from nc.gz file
        dataset = Dataset(file_path)
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max(dataset,file_path)
        dataset.close()

    def get_variables_min_max(self, datafile, filename):
        """
        :param nc: Dataset opened
        :param file_path: file path
        :return:
        """
        if '_01kmPro_' in filename:
            return self.get_01kmPro_01kmLay_metadata(datafile, filename)
        elif '_01kmLay_' in filename:
            return self.get_01kmPro_01kmLay_metadata(datafile, filename)
        elif '_ATB_' in filename:
            return self.get_ATB_metadata(datafile)

    def get_ATB_metadata(self, datafile):
        #extract metadata for *_ATB_* files
        lats = datafile['Latitude'][:].flatten()
        lons = datafile['Longitude'][:].flatten()
        ref_time = datetime(datetime.strptime(datafile['Date'][:].replace(' ',''),'%d%b%Y').year-1,12,31) 

        minTime, maxTime = [ref_time+timedelta(days=datafile['Start_JDay'][:].item()),
                            ref_time+timedelta(days=datafile['End_JDay'][:].item())]

        """ Due to calibration on 2020-02-23, files on this date contains several invalid
           latitude values; We decide to skip these data points
        """
        maxlat, minlat, maxlon, minlon = [lats[np.where(lats>0)].max(),
                                          lats[np.where(lats>0)].min(),
                                          lons[np.where(lats>0)].max(),
                                          lons[np.where(lats>0)].min()]

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_01kmPro_01kmLay_metadata(self, datafile, filename):
        #extract metadata for *_01kmPro_* and *_01kmLay_* files
        lats = datafile['geolocation']['CPL_Latitude'][:].flatten()
        lons = datafile['geolocation']['CPL_Longitude'][:].flatten()
        ref_time = datetime(int(datafile['metadata_parameters']['File_Year'][:])-1,12,31)

        if '_01kmPro_' in filename:
            key = 'profile'
        elif '_01kmLay_' in filename:
            key = 'layer_descriptor'

        minTime = ref_time + timedelta(datafile[key]['Profile_Decimal_Julian_Day'][:].min())
        maxTime = ref_time + timedelta(datafile[key]['Profile_Decimal_Julian_Day'][:].max())
        maxlat, minlat, maxlon, minlon = [lats[np.where(lats>0)].max(),
                                          lats[np.where(lats>0)].min(),
                                          lons[np.where(lats>0)].max(),
                                          lons[np.where(lats>0)].min()]

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
    print('Extracting cplimpacts  Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_CPL_L2_V1-02_01kmLay_20200115.hdf5"
    exnet = ExtractCplimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
