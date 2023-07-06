from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset


class ExtractSbuparsimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract sbuparsimpacts 
    """

    def __init__(self, file_path):
        # super().__init__(file_path)
        self.file_path = file_path
        # these are needed to metadata extractor
        self.gps_path = '../src/helpers/GPS.nc'
        self.lloc = {'2020-01-18':[-73.030, 40.965], '2020-01-19':[-73.030, 40.965],
                     '2020-01-25':[-73.127, 40.897], '2020-02-07':[-73.127, 40.897],
                     '2020-02-13':[-73.030, 40.965], '2020-02-24':[-73.127, 40.897],
                     '2020-02-25':[-73.127, 40.897], '2020-02-26':[-73.127, 40.897],
                     '2020-02-27':[-73.127, 40.897]} 

        #navigation info from sbuceilimpacts dataset
        self.gloc = {'SBU':[-73.127,40.897], 'Smith Point':[-72.862,40.733], 'Cedar Beach':[-73.030,40.965]}
        self.tperiod = [[datetime(2019,12,13,16,53), datetime(2019,12,13,19,53)],
                       [datetime(2020,1,18,16,8), datetime(2020,1,19,1,23)],
                       [datetime(2020,2,13,2,38), datetime(2020,2,13,11,8)]]
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
        if 'parsivel_2022' in file_path:
            minTime, maxTime, minlat, maxlat, minlon, maxlon = self.get_2022_metadata(nc)
        else: # 2020 data files
            if '_RT.nc' in file_path:
                minTime, maxTime, minlat, maxlat, minlon, maxlon = self.get_RT_metadata(nc)
            elif '_MAN.nc' in file_path:
                minTime, maxTime, minlat, maxlat, minlon, maxlon = self.get_MAN_SB_metadata(nc)
            elif '_SB.nc' in file_path:
                minTime, maxTime, minlat, maxlat, minlon, maxlon = self.get_MAN_SB_metadata(nc)

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_2022_metadata(self, nc):
        #2022 SBU fixed site (Pavlos's email): 40.89712, -73.12771.
        sb_loc_2022 = [-73.12771, 40.89712] #lon, lat

        timefield = np.array(nc.variables['UNIX_TIME'][:])
        minTime = datetime(1970,1,1) + timedelta(seconds=int(timefield.min()))
        maxTime = datetime(1970,1,1) + timedelta(seconds=int(timefield.max()))
        minlat, maxlat, minlon, maxlon = [sb_loc_2022[1]-(0.03/111.325),
                                          sb_loc_2022[1]+(0.03/111.325),
                                          sb_loc_2022[0]-(0.03/111.325),
                                          sb_loc_2022[0]+(0.03/111.325)]

        return minTime, maxTime, minlat, maxlat, minlon, maxlon


    def get_RT_metadata(self, nc):
        minTime, maxTime, minlat, maxlat, minlon, maxlon = [datetime(2100, 1, 1),
                                                            datetime(1900, 1, 1),
                                                            90.0, -90.0, 180.0, -180.0]
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

        #load data file
        timefield = np.array(nc.variables['UNIX_TIME'][:])

        for i in range(timefield.shape[0]):
            ltime = datetime(1970, 1, 1) + timedelta(seconds=int(timefield[i]))
            ltime_str = ltime.strftime('%Y-%m-%d-%H-%M')
            ltime_day = ltime.strftime('%Y-%m-%d')
            if ltime_day in self.lloc:
                south, north, west, east = [self.lloc[ltime_day][1]-0.01, self.lloc[ltime_day][1]+0.01,
                                                    self.lloc[ltime_day][0]-0.01, self.lloc[ltime_day][0]+0.01]
            elif ltime_str in loc:
                south, north, west, east = [loc[ltime_str][0],loc[ltime_str][1],
                                                   loc[ltime_str][2],loc[ltime_str][3]]
            else:
                site = 'SBU'
                for i in range(3):
                    if ltime >= self.tperiod[i][0] and ltime <= self.tperiod[i][1]:
                        site = self.site_loc[i]
                        break
                south, north, west, east = [self.gloc[site][1]-(0.03/111.325),
                                                    self.gloc[site][1]+(0.03/111.325),
                                                    self.gloc[site][0]-(0.03/111.325),
                                                    self.gloc[site][0]+(0.03/111.325)]

            #Update values for minTime, maxTime, minlat, maxlat, minlon, maxlon
            minTime = min(minTime, ltime)
            maxTime = max(maxTime, ltime)
            minlat = min(minlat, south)
            maxlat = max(maxlat, north)
            minlon = min(minlon, west)
            maxlon = max(maxlon, east)

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_MAN_SB_metadata(self, nc):
        man_sb_loc = {'MAN': [-74.0068,40.7282], 'SB': [-73.127,40.897]}
        minTime, maxTime = [datetime(2100, 1, 1), datetime(1900, 1, 1)]

        timefield = np.array(nc.variables['UNIX_TIME'][:])

        for i in range(timefield.shape[0]):
            ltime = datetime(1970, 1, 1) + timedelta(seconds=int(timefield[i]))
            minTime = min(minTime, ltime)
            maxTime = max(maxTime, ltime)

        site = self.file_path.split('/')[-1].split('_')[-1].split('.')[0]

        minlat, maxlat, minlon, maxlon = [man_sb_loc[site][1]-(0.03/111.325),
                                          man_sb_loc[site][1]+(0.03/111.325),
                                          man_sb_loc[site][0]-(0.03/111.325),
                                          man_sb_loc[site][0]+(0.03/111.325)]

        return minTime, maxTime, minlat, maxlat, minlon, maxlon

    def get_MAN_metadata(self, nc):
        minTime, maxTime = [datetime(2100, 1, 1), datetime(1900, 1, 1)]
        ctime = np.array(nc.variables['time'][:])
        refTime = datetime(1970, 1, 1)
        for i in range(ctime.shape[0]):
            cTime = refTime + timedelta(seconds=float(ctime[i]))
            minTime = cTime if cTime < minTime else minTime
            maxTime = cTime if cTime > maxTime else maxTime

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
