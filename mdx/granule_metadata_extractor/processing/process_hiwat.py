from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy as np
from netCDF4 import Dataset
from datetime import datetime, timedelta
import pygrib

class ExtractHiwatMetadata(ExtractNetCDFMetadata):
    """
    A class to extract HIWAT
    """

    def __init__(self, file_path):
        self.file_path = file_path

        filename = file_path.split('/')[-1]
        if '.grb2' in filename:
           self.fileformat = 'GRIB2'
           # extracting time and space metadata for GRIB2 file
           [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
               self.get_variables_min_max_grib2()
        else: #netCDF-3
           self.fileformat = 'netCDF-3'
           # extracting time and space metadata for netCDF-3 file
           [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
               self.get_variables_min_max_netcdf()

    def get_variables_min_max_netcdf(self):
        """
        :return:
        """
        """
        Extract temporal and spatial metadata from netCDF files
        """
        # open nc file
        nc = Dataset(self.file_path)

        #Example file: 'wrfout_d02_2017-04-04_06-00-00'
        #nc['XTIME'].units
        #'minutes since 2017-04-02 18:00:00'

        utc_minute = nc.variables['XTIME'][:]
        time_att = nc.variables['XTIME'].getncattr('units')
        utc_ref = datetime.strptime(time_att,'minutes since %Y-%m-%d %H:%M:%S')
        lat = nc.variables['XLAT'][:]
        lon = nc.variables['XLONG'][:]

        #missing values are set to nan
        maxlat, minlat, maxlon, minlon = [float(np.max(lat)), float(np.min(lat)),
                                          float(np.max(lon)), float(np.min(lon))]

        minTime = utc_ref + timedelta(minutes=float(np.min(utc_minute)))
        maxTime = utc_ref + timedelta(minutes=float(np.max(utc_minute)))

        nc.close()

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_variables_min_max_grib2(self):
        """
        :return:
        """
        grbs = pygrib.open(self.file_path)
        grbs.seek(0)

        #291:Latitude (-90 to +90):deg (instant):lambert:surface:level 0:fcst time 0 hrs:from 201704021800
        #292:East Longitude (0 - 360):deg (instant):lambert:surface:level 0:fcst time 0 hrs:from 201704021800

        selected_grb = grbs.select(name='Temperature')[0]
        data_actual, lats, lons = selected_grb.data()

        maxlat = np.nanmax(lats)
        minlat = np.nanmin(lats)
        maxlon = np.nanmax(lons)
        minlon = np.nanmin(lons)

        tkn = self.file_path.split('/')[-1].split('_')
        hour = int(tkn[-1][-4:])/100.
        if tkn[0] == 'variant': #i.e.,variant_HKH9_1803291800_wrfout_ens9_arw_d02.grb2f4800
           utc_ref = tkn[2]
        else: #i.e., HKH9_1803291800_wrfout_ens9_arw_d02.grb2f4800
           utc_ref = tkn[1]
        minTime = datetime.strptime('20'+utc_ref,'%Y%m%d%H%M')+timedelta(hours=hour)
        maxTime = minTime

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

    def get_metadata(self, ds_short_name, format='netCDF-3', version='1', **kwargs):
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
