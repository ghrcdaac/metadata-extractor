from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy as np
import numpy.ma as ma
import math
from datetime import datetime, timedelta
from netCDF4 import Dataset

class ExtractGoesrglmgridMetadata(ExtractNetCDFMetadata):
    """
    A class to extract GLM L3
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.fileformat = 'netCDF-4'

        # extracting time and space metadata from nc file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max()

    def convert_to_180(self,x):
        #if a lon value is outside (-180,180), convert it to within the range
        if x < -180:
           lon = x + 360.
        elif x > 180.:
           lon = x - 360.
        else:
           lon = x
        return lon

    def calc_lat_lon(self,xx,yy):
        #intermediate calculations
        a = (math.sin(xx))**2 + (math.cos(xx))**2 * ((math.cos(yy))**2 + r_eq**2/r_pol**2*(math.sin(yy))**2)
        b = -2 * H * math.cos(xx) * math.cos(yy)
        c = H**2 - r_eq**2

        if b**2-4*a*c < 0:
           lat = -999.
           lon = -999.
        else:
           #distance from the satellite to point P
           r_s = (-b-math.sqrt(b**2-4*a*c))/2/a

           #derived using satellite location and earth geometry s_x, s_y, s_z
           s_x = r_s * math.cos(xx) * math.cos(yy)
           s_y = -r_s * math.sin(xx)
           s_z = r_s * math.cos(xx) * math.sin(yy)

           #geodetic latitude and longitude for point P
           #math.atan(x) return the arc tangent of x, in radians. The result is between -pi/2 and pi/2
           lat = math.atan(((r_eq**2)/(r_pol**2)) * (s_z/math.sqrt((H-s_x)**2 + s_y**2))) #radians
           lon = lon0 - math.atan(s_y/(H-s_x)) #radians

           #convert radians to degrees
           lat = lat * 180./math.pi #degree
           lon = lon * 180./math.pi #degree

        return lat, lon

    def get_variables_min_max(self):
        global r_eq, r_pol, H, lon0

        data = Dataset(self.file_path)

        #radius of Earth at the equator in meters
        r_eq = data['goes_imager_projection'].semi_major_axis

        #radius of Earth at the pole in meters
        r_pol = data['goes_imager_projection'].semi_minor_axis

        #distance from satellite to earth center in meters
        H = data['goes_imager_projection'].perspective_point_height+r_eq

        #longitude of projection origin in units of radians
        lon0 = data['goes_imager_projection'].longitude_of_projection_origin * (math.pi/180.)

        # get time variable
        minTime =datetime.strptime(data.time_coverage_start,
                                '%Y-%m-%dT%H:%M:%SZ')
        maxTime =datetime.strptime(data.time_coverage_end,
                                '%Y-%m-%dT%H:%M:%SZ')

        # get bounding box 
        x = data['x'][:]
        y = data['y'][:]
        num_x = x.size
        num_y = y.size

        for i in range(0,num_x):
            tmp_lon = np.ones(num_y)
            for j in range(0,num_y):
                lat_p, lon_p = self.calc_lat_lon(x[i],y[j])
                tmp_lon[j] = lon_p

            #mask out -999. values
            mlon = ma.masked_values(tmp_lon,-999.)
            if mlon.count() > 0:
               wlon = self.convert_to_180(mlon.min())
               break

        for i in range(num_x-1,0,-1):
            tmp_lon = np.ones(num_y)
            for j in range(0,num_y):
                lat_p, lon_p = self.calc_lat_lon(x[i],y[j])
                tmp_lon[j] = lon_p

            #mask out -999. values
            mlon = ma.masked_values(tmp_lon,-999.)
            if mlon.count() > 0:
               elon = self.convert_to_180(mlon.max())
               break

        for j in range(0,num_y):
            tmp_lat = np.ones(num_x)
            for i in range(0,num_x):
                lat_p, lon_p = self.calc_lat_lon(x[i],y[j])
                tmp_lat[i] = lat_p

            #mask out -999. values
            mlat = ma.masked_values(tmp_lat,-999.)
            if mlat.count() > 0:
               nlat = mlat.max()
               break

        for j in range(num_y-1,0,-1):
            tmp_lat = np.ones(num_x)
            for i in range(0,num_x):
                lat_p, lon_p = self.calc_lat_lon(x[i],y[j])
                tmp_lat[i] = lat_p

            #mask out -999. values
            mlat = ma.masked_values(tmp_lat,-999.)
            if mlat.count() > 0:
               slat = mlat.min()
               break

        data.close()
        return minTime, maxTime, slat, nlat, wlon, elon

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
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
        data['AgeOffFlag'] = True if 'NRT' in data['GranuleUR'] else False
        return data


if __name__ == '__main__':
    print('Extracting goesrglmgrid Metadata')
    path_to_file = "../../test/fixtures/OR_GLM-L3-GLMF-M3_G17_e20200101235900.nc"
    exnet = ExtractGlml2Metadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
