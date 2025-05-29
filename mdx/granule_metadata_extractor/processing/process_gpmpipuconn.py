from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os, re
from datetime import datetime, timedelta

class ExtractGpmpipuconnMetadata(ExtractNetCDFMetadata):
    """
    A class to extract gpmpipuconn
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        self.instr_site = {'003':{'lat':41.808,'lon':-72.294},
                           '002A':{'lat':41.818,'lon':-72.258}, #PI002 before 20231101
                           '002B':{'lat':41.808,'lon':-72.294}, #PI002 after 20231101
                          }
        self.f_type = {'piv':'Binary','pv2':'Binary','png':'PNG','dat':'ASCII','log':'ASCII'}

        self.fileformat = 'ASCII'
        if file_path.endswith('dat'):
           self.fileformat = 'ASCII'
           [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max_ascii()
        else:
           [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max_others()


    def get_variables_min_max_ascii(self):
        """
        Extracts temporal and spatial metadata from the following files:
        """
        #Sample file: UConn_PIP_0032021110221400_a_p.dat
        fn = self.file_path.split('/')[-1]
        tkn = fn.split('.')[0].split('_')
        tmp = [x for x in tkn if len(x) > 12] #i.e., 0032021091515190

        with open(self.file_path, 'rb') as fp:
             encoded_lines = fp.readlines()

        lines = []
        for encoded_line in encoded_lines:
            err_flag = 0
            try:
                decoded_line = encoded_line.decode("utf-8")
            except:
                err_flag = 1

            if err_flag == 0:
               lines.append(decoded_line)

        utc = []
        for line in lines[10:]:
            tkn = line.split()
            if tkn[0] != '-99' and len(tkn) >13:
               utc_char = [tkn[6].zfill(4),tkn[7].zfill(2),tkn[8].zfill(2),tkn[9].zfill(2),tkn[10].zfill(2),tkn[11].zfill(2)]
               utc_str = ''.join(utc_char)
               err_flag = 0
               try:
                   utc0 = datetime.strptime(utc_str,'%Y%m%d%H%M%S')
               except:
                   err_flag = 1
               if err_flag == 0:
                  utc.append(utc0)
        if len(utc) == 0:
           if tmp[0].startswith('003'):
               start_time = datetime.strptime(tmp[0],'003%Y%m%d%H%M0')
           elif tmp[0].startswith('002'):
               start_time = datetime.strptime(tmp[0],'002%Y%m%d%H%M0')
           end_time = start_time + timedelta(seconds=599) #10 minutes
        else:
           start_time, end_time = [min(utc), max(utc)]

        if tmp[0].startswith('003'):
           lat = self.instr_site['003']['lat']
           lon = self.instr_site['003']['lon']
        elif tmp[0].startswith('002'):
           if end_time <= datetime(2023,11,1):
              lat = self.instr_site['002A']['lat']
              lon = self.instr_site['002A']['lon']
           else:
              lat = self.instr_site['002B']['lat']
              lon = self.instr_site['002B']['lon']

        north = lat + 0.01
        south = lat - 0.01
        east = lon + 0.01
        west = lon - 0.01

        print(self.file_path, start_time, end_time, north, south, east, west)

        return start_time, end_time, south, north, west, east

    def get_variables_min_max_others(self):
        fn = self.file_path.split('/')[-1]
        self.fileformat = self.f_type[fn.split('.')[-1]] #i.e., 'piv':'Binary', or 'pv2':'Binary'

        tkn = fn.split('.')[0].split('_')
        tmp = [x for x in tkn if len(x) > 12] #i.e., 0032021091515190 (piv or pv2) or 20210915T155229 (log)

        if fn.endswith('piv') or fn.endswith('pv2') or fn.endswith('png'): #Raw_Video
           utc_str = tmp[0]
           if utc_str.startswith('003'):
               site_key = '003'
               start_time = datetime.strptime(utc_str,'003%Y%m%d%H%M0')
               end_time = start_time + timedelta(seconds=599)
           elif utc_str.startswith('002'):
               start_time = datetime.strptime(utc_str,'002%Y%m%d%H%M0')
               end_time = start_time + timedelta(seconds=599) #10 minutes
               if end_time <= datetime(2023,11,1):
                   site_key = '002A'
               else:
                   site_key = '002B'

        elif fn.endswith('log'):
           #UConn_PIP_PIP003_IMP_20210915T155229.log
           start_time = datetime.strptime(tmp[0],'%Y%m%dT%H%M%S')
           end_time = datetime.strptime(start_time.strftime('%Y%m%d'),'%Y%m%d')+timedelta(seconds=3600*24-1)
           if 'PIP003' in tkn:
               site_key = '003'
           else: #'PIP002
               if end_time <= datetime(2023,11,1):
                  site_key = '002A'
               else:
                  site_key = '002B'
        lat = self.instr_site[site_key]['lat']
        lon = self.instr_site[site_key]['lon']

        north = lat + 0.01
        south = lat - 0.01
        east = lon + 0.01
        west = lon - 0.01
        print(self.file_path,start_time,end_time,north,south,east,west)

        return start_time, end_time, south, north, west, east


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
        return data
