from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset


class ExtractSbuceilimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract sbuceilimpacts 
    """

    def __init__(self, file_path):
        # super().__init__(file_path)
        self.file_path = file_path
        # these are needed to metadata extractor
        self.gps_path = '../src/helpers/GPS.nc'
        self.gloc = {'SBU': [-73.127, 40.897], 'Smith Point': [-72.862, 40.733],
                     'Cedar Beach': [-73.030, 40.965]}
        self.tperiod = [[datetime(2019, 12, 13, 16, 53), datetime(2019, 12, 13, 19, 53)],
                        [datetime(2020, 1, 18, 16, 8), datetime(2020, 1, 19, 1, 23)],
                        [datetime(2020, 2, 13, 2, 38), datetime(2020, 2, 13, 11, 8)]]
        self.site_loc = ['Smith Point', 'Cedar Beach', 'Cedar Beach']
        self.fileformat = 'netCDF-3'

        # extracting time and space metadata from nc.gz file
        dataset = Dataset(file_path)
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
            self.get_variables_min_max(dataset, file_path)
        dataset.close()

    def get_variables_min_max(self, nc, file_path):
        """
        :param nc: Dataset opened
        :param file_path: file path
        :return:
        """
        if '_RT.nc' in file_path:
            minTime, maxTime, minlat, maxlat, minlon, maxlon = self.get_RT_metadata(nc)
        elif '_BNL.nc' in file_path:
            minTime, maxTime, minlat, maxlat, minlon, maxlon = self.get_BNL_metadata(nc)
        elif '_MAN.nc' in file_path:
            minTime, maxTime, minlat, maxlat, minlon, maxlon = self.get_MAN_metadata(nc)
        elif '_cl51k.nc' in file_path:
            minTime, maxTime, minlat, maxlat, minlon, maxlon = self.get_SB_metadata(nc)

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_RT_metadata(self, nc):
        minlat, maxlat, minlon, maxlon = [90.0, -90.0, 180.0, -180.0]
        # load GPS.nc file for lat and lon info

        gpsnc = Dataset(os.path.join(os.path.dirname(__file__), self.gps_path))
        ctime = np.array(gpsnc.variables['time'][:])
        lat = np.array(gpsnc.variables['lat'][:])
        lon = np.array(gpsnc.variables['lon'][:])
        gpsnc.close()
        # the resolution of time is several tiem step in an hour, so look-up-table
        # is only at hour
        loc = {}
        for i in range(ctime.shape[0]):
            ltime = datetime(1970, 1, 1) + timedelta(seconds=int(ctime[i]))
            llat = float(lat[i])
            llon = float(lon[i])
            ltime_str = ltime.strftime('%Y-%m-%d-%H-%M')
            if ltime_str in loc:
                loc[ltime_str][0] = llat if llat < loc[ltime_str][0] else loc[ltime_str][0]
                loc[ltime_str][1] = llat if llat > loc[ltime_str][1] else loc[ltime_str][1]
                loc[ltime_str][2] = llon if llon < loc[ltime_str][2] else loc[ltime_str][2]
                loc[ltime_str][3] = llon if llon > loc[ltime_str][3] else loc[ltime_str][3]
            else:
                loc[ltime_str] = [llat, llat, llon, llon]

        # load data file
        timefield = np.array(nc.variables['time'][:])
        tt_units = nc['time'].units #   units: seconds since 1904-01-01 00:00:00.000 00:00 
        tkn = tt_units.split()
        dt0 = datetime.strptime(f"{tkn[2]}{tkn[3]}",'%Y-%m-%d%H:%M:%S.%f')

        minTime = dt0 + timedelta(seconds=int(min(timefield)))
        maxTime = dt0 + timedelta(seconds=int(max(timefield)))

        for i in range(timefield.shape[0]):
            ltime = dt0 + timedelta(seconds=int(timefield[i]))
            ltime_str = ltime.strftime('%Y-%m-%d-%H-%M')
            if ltime_str in loc:
                [mnlat, mxlat, mnlon, mxlon] = [loc[ltime_str][0], loc[ltime_str][1],
                                                loc[ltime_str][2], loc[ltime_str][3]]
            else:
                site = 'SBU'
                for i in range(3):
                    if ltime >= self.tperiod[i][0] and ltime <= self.tperiod[i][1]:
                        site = self.site_loc[i]
                        break
                [mnlat, mxlat, mnlon, mxlon] = [self.gloc[site][1] - (0.03 / 111.325),
                                                self.gloc[site][1] + (0.03 / 111.325),
                                                self.gloc[site][0] - (0.03 / 111.325),
                                                self.gloc[site][0] + (0.03 / 111.325)]

            maxlat = mxlat if mxlat > maxlat else maxlat
            minlat = mnlat if mnlat < minlat else minlat
            maxlon = mxlon if mxlon > maxlon else maxlon
            minlon = mnlon if mnlon < maxlon else maxlon

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_BNL_metadata(self, nc):

        tt = np.array(nc['time'][:])
        tt_units = nc['time'].units #    Units: hours since 2020-01-01 00:00:00 00:00 
        tkn = tt_units.split()
        dt0 = datetime.strptime(f"{tkn[2]}{tkn[3]}",'%Y-%m-%d%H:%M:%S')

        minTime = dt0 + timedelta(hours=float(min(tt)))
        maxTime = dt0 + timedelta(hours=float(max(tt)))

        lat = float(nc.variables['latitude'][:])
        lon = float(nc.variables['longitude'][:])

        maxlat, minlat, maxlon, minlon = [lat + (0.03 / 111.325), lat - (0.03 / 111.325),
                                          lon + (0.03 / 111.325), lon - (0.03 / 111.325)]

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_MAN_metadata(self, nc):
        tt = np.array(nc['time'][:])
        tt_units = nc['time'].Units #    Units: days since 1970-01-01 00:00:00.000
        tkn = tt_units.split()
        dt0 = datetime.strptime(f"{tkn[2]}{tkn[3]}",'%Y-%m-%d%H:%M:%S.%f')
        minTime = dt0 + timedelta(seconds=int(min(tt))) #note: Units says 'days' but actually 'seconds'
        maxTime = dt0 + timedelta(seconds=int(max(tt)))

        lat = nc.variables['Location_latitude'][:]
        lon = nc.variables['Location_longitude'][:]

        maxlat, minlat, maxlon, minlon = [float(max(lat)) + (0.03 / 111.325),
                                          float(max(lat)) - (0.03 / 111.325),
                                          float(max(lon)) + (0.03 / 111.325),
                                          float(max(lon)) - (0.03 / 111.325)]
        self.fileformat = 'netCDF-4'

        return minTime, maxTime, minlat, maxlat, minlon, maxlon


    def get_SB_metadata(self, nc):
        tt = np.array(nc['time'][:])
        tt_units = nc['time'].Units #   Units: seconds since 1970-01-01 00:00:00.000 
        tkn = tt_units.split()
        dt0 = datetime.strptime(f"{tkn[2]}{tkn[3]}",'%Y-%m-%d%H:%M:%S.%f')
        minTime = dt0 + timedelta(seconds=int(min(tt))) #note: Units says 'days' but actually 'seconds'
        maxTime = dt0 + timedelta(seconds=int(max(tt)))

        lat = nc.variables['Location_latitude'][:]
        lon = nc.variables['Location_longitude'][:]

        maxlat, minlat, maxlon, minlon = [float(max(lat)) + (0.03 / 111.325),
                                          float(max(lat)) - (0.03 / 111.325),
                                          float(max(lon)) + (0.03 / 111.325),
                                          float(max(lon)) - (0.03 / 111.325)]
        self.fileformat = 'netCDF-4'

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
    print('Extracting sbuceilimpacts  Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_SBU_ceilo_20200104_ct25k_BNL.nc"
    exnet = ExtractSbuceilimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
