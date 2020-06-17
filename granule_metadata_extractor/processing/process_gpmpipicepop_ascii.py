from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime,  timedelta
from zipfile import ZipFile
import io


class ExtractGpmpipicepopASCIIMetadata(ExtractASCIIMetadata):
    """
    A class to extract Gpmpipicepop ASCII
    """
    max_lat = -90.0
    min_lat = 90.0
    max_lon = -180.0
    min_lon = 180.0
    # Geolocation for PIP#002 at KO1 station
    lat2 = 37.738157
    lon2 = 128.805847

    # Geolocation for PIP#003 at KO2 station
    lat3 = 37.665208
    lon3 = 128.699611

    minTime = datetime(2100, 1, 1)
    maxTime = datetime(1900, 1, 1)

    def __init__(self, file_path):
        super().__init__(file_path)
        self.file_name = file_path.split('/')[-1]
        self.get_variables_min_max(file_path)

    def get_variables_min_max(self, variable_key):
        """

        :param c_data: netCDF data
        :param variable_key: The NetCDF key we need to target
        :return: list longitude coordinates
        """

        if "_002_" in self.file_name:  # Site 1
            self.max_lat = self.lat2
            self.min_lat = self.lat2
            self.max_lon = self.lon2
            self.min_lon = self.lon2
        elif "_003_" in self.file_name:  # Site 2
            self.max_lat = self.lat3
            self.min_lat = self.lat3
            self.max_lon = self.lon3
            self.min_lon = self.lon3

        if "_YTD_dat.zip" in self.file_name:
            self.get_zip_YTD_metadata(variable_key)
        elif "_Daily_dat.zip" in self.file_name:
            self.get_zip_daily_metadata(variable_key)
        elif "_q.dat" in self.file_name:
            self.get_q_dat_metadata(variable_key)

        return

    def get_q_dat_metadata(self, filename):
        """

        :param filename:
        :return:
        """
        with open(filename) as dat_file:
            lines = dat_file.readlines()

        tkn0 = lines[5].split()
        yr_str = tkn0[0]
        mon_str = tkn0[1]
        day_str = tkn0[2]
        hr_str = tkn0[3]
        dt = []
        for ii in range(9, len(lines)):
            tkn = lines[ii].split()

            min_str = tkn[0]
            sec_str = tkn[1]

            if yr_str == '-99' or mon_str == '-99' or day_str == '-99' or hr_str == '-99' or min_str == '-99' or sec_str == '-99':
                continue

            if len(mon_str) == 1:
                mon_str = '0' + mon_str
            if len(day_str) == 1:
                day_str = '0' + day_str
            if len(hr_str) == 1:
                hr_str = '0' + hr_str
            if len(min_str) == 1:
                min_str = '0' + min_str
            if len(sec_str) == 1:
                sec_str = '0' + sec_str

            dt_str = yr_str + mon_str + day_str + hr_str + min_str + sec_str
            dt.append(datetime.strptime(dt_str, '%Y%m%d%H%M%S'))

        # If no valid time info found:
        if len(dt) == 0:
            tkn = self.file_name.split('_')
            self.minTime = datetime.strptime(tkn[3], '%Y%m%d%H%M')
            self.maxTime = self.minTime + timedelta(seconds=60)
        else:
            self.minTime = min(dt)
            self.maxTime = max(dt)
        return

    def get_zip_daily_metadata(self, file_path):
        archive = ZipFile(file_path, 'r')
        files = archive.namelist()
        for items in files:
            self.file_lines = archive.open(items)
            self.file_lines = io.TextIOWrapper(self.file_lines).readlines()
            rr = self.get_zip_Daily_dat_timeinfo(items)
            if rr['start'] < self.minTime:
                self.minTime = rr['start']
            if rr['end'] > self.maxTime:
                self.maxTime = rr['end']
        archive.close()

        return

    def get_zip_Daily_dat_timeinfo(self,filename):
        r = {'start': datetime(2100, 1, 1), 'end': datetime(1900, 1, 1)}
        start_line = 0
        if "_P_Minute" in filename:
            format = 1
            start_line = 9

        elif "_dsd." in filename or "_rho_Plots_D_" in filename or "_vvd_" in filename:
            format = 2  # Read YYYY, MM, DD from Line 5 (start from 0)
            tkn0 = self.file_lines[5].split()
            yr_str0 = tkn0[0]
            mon_str0 = tkn0[1]
            day_str0 = tkn0[2]
            start_line = 12  # Read hour and min from line 12

        dt = []
        for ii in range(start_line, len(self.file_lines)):
            tkn = self.file_lines[ii].split()
            if format == 1:
                yr_str = tkn[0]
                day_of_year_str = tkn[1]
                hr_str = tkn[2]
                min_str = tkn[3]

                if yr_str == '-99' or day_of_year_str == '-99' or hr_str == '-99' or min_str == '-99':
                    # print(fname, ii, yr_str,day_of_year_str, hr_str, min_str)
                    continue
                    # sys.exit()
                if len(day_of_year_str) == 1:
                    day_of_year_str = '00' + day_of_year_str
                elif len(day_of_year_str) == 2:
                    day_of_year_str = '0' + day_of_year_str

                if len(hr_str) == 1:
                    hr_str = '0' + hr_str
                if len(min_str) == 1:
                    min_str = '0' + min_str

                dt_str = yr_str + day_of_year_str + hr_str + min_str
                dt.append(datetime.strptime(dt_str, '%Y%j%H%M'))

            elif format == 2:
                yr_str = yr_str0
                mon_str = mon_str0
                day_str = day_str0
                hr_str = tkn[1]
                min_str = tkn[2]
                print(hr_str, min_str)
                if yr_str == '-99' or mon_str == '-99' or day_str == '-99' or hr_str == '-99' or min_str == '-99':
                    # print(fname, ii, yr_str,mon_str,day_str, hr_str, min_str)
                    continue
                    # sys.exit()

                if len(mon_str) == 1:
                    mon_str = '0' + mon_str
                if len(day_str) == 1:
                    day_str = '0' + day_str
                if len(hr_str) == 1:
                    hr_str = '0' + hr_str
                if len(min_str) == 1:
                    min_str = '0' + min_str

                dt_str = yr_str + mon_str + day_str + hr_str + min_str
                print(dt_str)
                dt.append(datetime.strptime(dt_str, '%Y%m%d%H%M'))
        if len(dt) == 0:
            tkn = self.file_name.split('_')
            r['start'] = datetime.strptime(tkn[3], '%Y%m%d')
            r['end'] = r['start'] + timedelta(seconds=86399)
        else:
            r['start'] = min(dt)
            r['end'] = max(dt)
        return r


    def get_zip_YTD_metadata(self, file_path):
        archive = ZipFile(file_path, 'r')
        files = archive.namelist()
        for items in files:
            self.file_lines = archive.open(items)
            self.file_lines = io.TextIOWrapper(self.file_lines).readlines()

            rr = self.get_zip_YTD_dat_timeinfo(items)
            if rr['start'] < self.minTime:
                self.minTime = rr['start']
            if rr['end'] > self.maxTime:
                self.maxTime = rr['end']
        archive.close()

        return

    def get_zip_YTD_dat_timeinfo(self, filename):
        r = {'start': datetime(2100, 1, 1), 'end': datetime(1900, 1, 1)}
        start_line = 0
        if "_P_" in filename or "_R_" in filename:
            start_line = 9
        elif "_eDen_" in filename or "_FallV_" in filename or "_PSD_" in filename:
            start_line = 10

        dt = []
        for ii in range(start_line, len(self.file_lines)):
            tkn = self.file_lines[ii].split()
            yr_str = tkn[0]
            mon_str = tkn[2]
            day_str = tkn[3]
            hr_str = tkn[4]
            min_str = tkn[5]

            if yr_str == '-99' or mon_str == '-99' or day_str == '-99' or hr_str == '-99' or min_str == '-99':
                continue

            if len(mon_str) == 1:
                mon_str = '0' + mon_str
            if len(day_str) == 1:
                day_str = '0' + day_str
            if len(hr_str) == 1:
                hr_str = '0' + hr_str
            if len(min_str) == 1:
                min_str = '0' + min_str

            dt_str = yr_str + mon_str + day_str + hr_str + min_str
            dt.append(datetime.strptime(dt_str, '%Y%m%d%H%M'))

        if len(dt) == 0:
            tkn = self.file_name.split('_')
            r['start'] = datetime.strptime(tkn[3], '%Y')
            r['end'] = r['start'] + timedelta(seconds=86399)
        else:
            r['start'] = min(dt)
            r['end'] = max(dt)
        return r


    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a netCDF file
        :param nc_data: netCDF data
        :param timestamp:  The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in [self.max_lat, self.min_lat, self.max_lon, self.min_lon]]
        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]

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

        start_date = self.minTime.strftime(date_format)
        stop_date = self.maxTime.strftime(date_format)
        return start_date, stop_date


    def get_metadata(self, ds_short_name, format='ASCII', version='01'):
        """

        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        start_date, stop_date = self.get_temporal(time_variable_key='lon',
                                                                              units_variable='time',
                                                                              date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        gemetry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data

if __name__ == '__main__':
    print('Extracting Gpmpipicepop ASCII Metadata')
