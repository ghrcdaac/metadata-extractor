from ..src.extract_browse_metadata import ExtractBrowseMetadata
import json
from os import path
from datetime import datetime, timedelta
import tarfile


er2_loc = {}
file_name_list = []
er2_lat = []
er2_lon = []
north = 0
south = 0
west = 0
east = 0
start_time = 0
end_time = 0


class ExtractEr2edopMetadata(ExtractBrowseMetadata):
    """
    A class to extract Er2edop
    """

    def __init__(self, file_path):
        global er2_loc, file_name_list, er2_lat, er2_lon
        super().__init__(file_path)

        with open(path.join(path.dirname(__file__), f"../src/helpers/er2edopRefData.json"), 'r') as fp:
            er2_loc = json.load(fp)

        for key in er2_loc.keys():
            er2_lat.append(er2_loc[key]['lat'])
            er2_lon.append(er2_loc[key]['lon'])

        if '.tar' in file_path:
            file_name_list = tarfile.open(file_path).getnames()
        else:
            file_name_list.append(file_path)
        # metadata = {}
        print(len(file_name_list))
        for itms in file_name_list:
            metadata = ExtractEr2edopMetadata.parse_browse_files(itms)
            self.get_variables_min_max(metadata)

    def get_variables_min_max(self, data):
        """

        :param data:
        :return:
        """
        global north, south, east, west, start_time, end_time

        data_start = datetime.strptime(str(data['start']), '%Y-%m-%d %H:%M:%S')
        data_end = datetime.strptime(str(data['end']), '%Y-%m-%d %H:%M:%S')

        if not north:
            north = data['NLat']
        if not south:
            south = data['SLat']
        if not east:
            east = data['ELon']
        if not west:
            west = data['WLon']
        if not start_time:
            start_time = data_start
        if not end_time:
            end_time = data_end

        north = data['NLat'] if (north <= data['NLat']) else north
        south = data['SLat'] if (south >= data['SLat']) else south
        east = data['ELon'] if (east <= data['ELon']) else east
        west = data['WLon'] if (west >= data['WLon']) else west
        end_time = data_end if (end_time <= data_end) else end_time
        start_time = data_start if (start_time >= data_start) else start_time
        print(north, south, east, west, start_time, end_time)
        return

    @staticmethod
    def find_er2_flight_track(t0, t1):
        global er2_loc
        current_lat = []
        current_lon = []
        for key in er2_loc.keys():
            dt = datetime.strptime(key, '%Y%m%d%H%M%S')
            if dt >= t0 and dt <= t1:
                current_lat.append(er2_loc[key]['lat'])
                current_lon.append(er2_loc[key]['lon'])
        return current_lat, current_lon

    @staticmethod
    def parse_browse_files(file_name_path):
        """

        :param file_name_path:
        :return:
        """
        global er2_lat, er2_lon
        r_ignore = {"NLat": -90, "SLat": 90, "ELon": -180, "WLon": 180,
                    "start": '2050-01-01 00:00:00', "end": '1970-01-01 00:00:00'}

        er2_lat_min = min(er2_lat)
        er2_lat_max = max(er2_lat)
        er2_lon_min = min(er2_lon)
        er2_lon_max = max(er2_lon)
        clon =[]
        clat =[]
        md = {}
        fname = file_name_path.split('/')[-1]
        # print(fname)

        tkn = fname.split('.')[0].split('_')
        date_str = '19' + tkn[1]  # 19980805

        if tkn[2] == '1444-138':
            # print(fname,' ignore this file and return!')
            md = r_ignore
            return md

        utc0 = int(tkn[2].split('-')[0])
        utc1 = int(tkn[2].split('-')[1])
        utc_str0 = "%04d" % utc0
        utc_str1 = "%04d" % utc1

        dt0 = datetime.strptime(date_str + utc_str0, '%Y%m%d%H%M')
        if utc0 <= utc1:
            dt1 = datetime.strptime(date_str + utc_str1, '%Y%m%d%H%M')
        else:
            # print(fname,'continued measurements to next day')
            next_day = datetime.strptime(date_str, '%Y%m%d') + timedelta(days=1)
            dt1 = datetime.strptime(datetime.strftime(next_day, '%Y%m%d') + utc_str1, '%Y%m%d%H%M')

        if date_str == '19980902':
            md["NLat"] = er2_lat_max
            md["SLat"] = er2_lat_min
            md["ELon"] = er2_lon_max
            md["WLon"] = er2_lon_min
        else:
            clat.clear()
            clon.clear()
            clat, clon = ExtractEr2edopMetadata.find_er2_flight_track(dt0, dt1)

            if len(clat) > 0 and len(clon) > 0:
                md["NLat"] = max(clat)
                md["SLat"] = min(clat)
                md["ELon"] = max(clon)
                md["WLon"] = min(clon)
            else:  # ER2 info not found
                # print(fname,len(clat),len(clon),' first try')
                next_day = datetime.strptime(date_str, '%Y%m%d') + timedelta(days=1)
                dt0 = datetime.strptime(datetime.strftime(next_day, '%Y%m%d') + utc_str0, '%Y%m%d%H%M')
                dt1 = datetime.strptime(datetime.strftime(next_day, '%Y%m%d') + utc_str1, '%Y%m%d%H%M')

                clat.clear()
                clon.clear()
                clat, clon = ExtractEr2edopMetadata.find_er2_flight_track(dt0, dt1)

                if len(clat) > 0 and len(clon) > 0:
                    md["NLat"] = max(clat)
                    md["SLat"] = min(clat)
                    md["ELon"] = max(clon)
                    md["WLon"] = min(clon)
                else:
                    print(fname, len(clat), len(clon), ' second try')
                    md = r_ignore
                    return md

        md["start"] = dt0
        md["end"] = dt1

        return md

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a GIF file
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        global north, south, east, west
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in [north, south, east, west]]
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
        global start_time, end_time
        start_date = datetime.strptime(str(start_time), '%Y-%m-%d %H:%M:%S')
        start_date = start_date.strftime(date_format)

        stop_date = datetime.strptime(str(end_time), '%Y-%m-%d %H:%M:%S')
        stop_date = stop_date.strftime(date_format)

        return start_date, stop_date


    def get_metadata(self, ds_short_name, format='GIF', version='01'):
        """

        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        start_date, stop_date = self.get_temporal(time_variable_key='lon', units_variable='time',
                                                  date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry()
        print(geometry_list)
        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(str(x) for x in geometry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data

if __name__ == '__main__':
    print('Extracting ER2edop Metadata')
    file_path = "/Users/amarouane/workstation/gitlab/granule-metadata-extractor/test/fixtures/camex3_er2edop_1998.220_daily.tar"
    exnet = ExtractEr2edopMetadata(file_path)
    metada = exnet.get_metadata("test")
    # with open('the_filename.txt', 'rb') as f:
    #     my_list = pickle.load(f)
    #
    # for itm in my_list:
    #     print(exnet.get_wnes_geometry(itm))