# This class is for processing goesrpltcrs netCDF files
import sys
from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
from datetime import datetime, timedelta
import re
from time import time

class ExtractGeosrpltcrsMetadata(ExtractNetCDFMetadata):

    def xi_get_temporal(self, time_variable_key='time', units_variable='units',  scale_factor=1.0, offset=0,
                     date_format = '%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The NetCDF variable we need to target
        :param units_variable: The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """
        tmbuf = self.variables[time_variable_key]
        stTime = datetime(2100, 1, 1)
        endTime = datetime(1900, 1, 1)
        ymd = self.file_path.split('_')[3]
        st_hr = -1.0
        for i in range(len(tmbuf)):
            if not isinstance(float(tmbuf[i]), float):
                #print(tmbuf[i])
                continue
            hr = float(tmbuf[i])
            if st_hr < 0.0:
                st_hr = hr
            if (st_hr - hr) > 5.0:
                hr = hr + 24.0  #next day
            dt = datetime.strptime(ymd, '%Y%m%d') + timedelta(hours=hr)
            if dt < stTime:
                stTime = dt
            if dt > endTime:
                endTime = dt
            
        return stTime, endTime
    def get_temporal(self, time_variable_key='time', units_variable='units',  scale_factor=1.0, offset=0,
                     date_format = '%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The NetCDF variable we need to target
        :param units_variable: The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """
        #reg_ex = r'(.*) since (.*)'
        unit = getattr(self.variables[time_variable_key], units_variable)
        reg_ex = r'.*_(\d{8})_.*.nc'
        extract_date_time = re.search(reg_ex, self.file_path).group
        ymd = extract_date_time(1)
        group = (unit, ymd)
        min_data, max_data = self.get_variables_min_max(time_variable_key)
        args_min = {group[0]: (scale_factor*min_data) + offset}
        args_max = {group[0]: (scale_factor * max_data) + offset}
        start_date = self.get_updated_date(group[1], **args_min)
        stop_date = self.get_updated_date(group[1], **args_max)
        if date_format:
            start_date, stop_date = start_date.strftime(date_format), stop_date.strftime(date_format)
        return start_date, stop_date




if __name__ == '__main__':
    print("Dependencies works")

    file_path = "/Users/amarouane/workStation/ops/ghrc-deploy/tasks/processing_tasks/metadata_extractor/test/fixtures/GOESR_L1B_20170411_v0.nc"
    exnet = ExtractGeosrpltcrsMetadata(file_path)
    # print(exnet.get_checksum())
    # print(exnet.get_file_size_megabytes())
    # target_variable = 'time'
    units_variable = 'units'
    # uni = exnet.get_units(nc, target_variable)
    # print(uni)
    #
    #
    #exnet.get_temporal(units_variable=units_variable))
    # print(time_elapsed)


    start, stop = exnet.get_temporal(units_variable=units_variable) #get_temporal(units_variable=units_variable)
    
    print(start, stop)

    # target_variable_lat1 = 'lat'
    # target_variable_lon1 = 'lon'
    # K = exnet.get_wnes_geometry(target_variable_lon1, target_variable_lat1)

    # print(K)

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
