from ..src.extract_browse_metadata import ExtractBrowseMetadata
import json
from os import path
from datetime import datetime, timedelta

north = 0
south = 0
west = 0
east = 0
start_time = 0
end_time = 0


class ExtractGoesrpltmisrepMetadata(ExtractBrowseMetadata):
    """
    A class to extract Goesrpltmisrep 
    """

    def __init__(self, file_path):
        super().__init__(file_path)
        md = {}
        md["SLat"] = 26.449
        md["NLat"] = 43.573
        md["WLon"] = -124.625
        md["ELon"] = -72.202
        md["start"] = datetime(2017,3,13)
        md["end"] = datetime(2017,5,17,23,59,59)
        self.get_variables_min_max(md)

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

        return

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a file
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


    def get_metadata(self, ds_short_name, format='PNG', version='1'):
        """

        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        format = 'PDF' if '.pdf' in self.file_path else format
        format = 'MS Word' if '.docx' in self.file_path else format
        format = 'MS Excel' if '.xlsx' in self.file_path else format
        format = 'KMZ' if '.kmz' in self.file_path else format
        start_date, stop_date = self.get_temporal(time_variable_key='lon', units_variable='time',
                                                  date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry()
        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(str(x) for x in geometry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data

if __name__ == '__main__':
    print('Extracting GOESRPLTMISREP Metadata')
    file_path = "../../test/fixtures/GOES-R_flight-report_20170321.pdf"
    exnet = ExtractGoesrpltmisrepMetadata(file_path)
    meta_data = exnet.get_metadata("test")
    print(meta_data)
    # with open('the_filename.txt', 'rb') as f:
    #     my_list = pickle.load(f)
    #
    # for itm in my_list:
    #     print(exnet.get_wnes_geometry(itm))
