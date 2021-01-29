from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset
from pyhdf.HDF import *
from pyhdf.VS import *

class ExtractIsslisgv1Metadata(ExtractNetCDFMetadata):
    """
    A class to extract both isslisg_v1_nqc and isslisg_v1_nrt 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        if file_path.endswith('.hdf'):
            self.fileformat = 'HDF-4'
        elif file_path.endswith('.nc'):
            self.fileformat = 'netCDF-4'

        # extracting time and space metadata from nc.gz file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        :return:
        """
        if self.file_path.endswith('.nc'):
            return self.get_nc_metadata()
        elif self.file_path.endswith('.hdf'):
            return self.get_hdf_metadata()

    def get_nc_metadata(self):
        datafile = Dataset(self.file_path)
        ref_time = datetime(1993,1,1) #TAI93 time
        utc = datafile['bg_info_TAI93_time'][:]
        minTime, maxTime = [ref_time+timedelta(seconds=utc.min()),
                            ref_time+timedelta(seconds=utc.max())]

        #pre-define bounding box same as on-prem bounding box hard-coded by Nathan
        maxlat, minlat, maxlon, minlon = [55., -55., 180., -180.]

        if 'bg_data_summary_' in ' '.join(datafile.variables.keys()):
            lat = np.array(datafile['bg_data_summary_boresight'][:,0])
            lon = np.array(datafile['bg_data_summary_boresight'][:,1])

            #some files contain  -90. latitude values which are incorrect
            #i.e., ISS_LIS_BG_V1.0_20190408_NQC_12868.nc
            #We need filter such value(s) out as well as their corresponding longitude values
            idx = np.where(lat != -90.)[0]
            if len(idx) > 0:
              maxlat, minlat = [lat[idx].max(),lat[idx].min()]
              maxlon, minlon = [lon[-1],lon[0]]

        datafile.close()
        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_hdf_metadata(self):
        f = HDF(self.file_path)
        vs = f.vstart()
        data = vs.attach('bg_info')
        bg_info = data[:]#['TAI93_time', 'alert_summary',......,'bg_value', 'noise_index', 'event_count']

        bg_info_TAI93_time = [bg_info[i][0] for i in range(len(bg_info))]
        utc = np.array(bg_info_TAI93_time)

        ref_time = datetime(1993,1,1) #TAI93 time
        minTime, maxTime = [ref_time+timedelta(seconds=utc.min()),
                            ref_time+timedelta(seconds=utc.max())]
        data.detach()

        #pre-define bounding box same as on-prem bounding box hard-coded by Nathan
        maxlat, minlat, maxlon, minlon = [55., -55., 180., -180.]

        data = vs.attach('bg_data_summary')
        #Info about 'bg_data_summary' as below:
        #member index 2
        #vdata: bg_data_summary tag,ref: 1962 5
        #fields: ['TAI93_time', 'address', 'boresight', 'corners']
        #nrecs: 105 #this number varies with files
        #'boresight' contains lat/lon values
        try:
           bg_data_summary = data[:] #list
           boresight = np.array([bg_data_summary[i][2] for i in range(len(bg_data_summary))])
           lat = boresight[:,0]
           lon = boresight[:,1]
           idx = np.where(lat != -90.)[0]
           if len(idx) > 0:
              maxlat, minlat = [lat[idx].max(),lat[idx].min()]
              maxlon, minlon = [lon[-1],lon[0]]
        except HDF4Error: # end of vdata reached
                          #corresponding *.nc doesn't have bg_data_summary_[lat|lon|corners|TAI93_time] variables
                          #use on-prem bounding box (hard-coded by Nathan)
           pass
    
        data.detach()
        vs.end()
        f.close()

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
        data['AgeOffFlag'] = True if 'NRT' in data['GranuleUR'] else None
        return data


if __name__ == '__main__':
    print('Extracting isslis_v1_nqc or isslis_v1_nrt Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_CPL_L2_V1-02_01kmLay_20200115.hdf5"
    exnet = ExtractIsslisv1Metadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
