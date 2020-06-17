from .metadata_extractor import MetadataExtractor
import csv
import re
from dateutil.parser import parse
from hashlib import md5

class ExtractCSVMetadata(MetadataExtractor):
    """
    Class to extract netCDF metadata
    """
    def __init__(self, file_path):
        super().__init__(file_path)

    def __get_min_max(self, reader, culumn_position):
        """

        :param reader: csv cursor reader
        : culumn_position: Position of the column you want extract min, max
        :return: min_value, max_value
        """
        min_val, max_val = (None, None)
        for row in reader:
            try:
                first = float(row[culumn_position].lstrip())
                if(None in [min_val, max_val]):
                    max_val, min_val = (first,first)
                max_val = first if(max_val <= first) else max_val
                min_val = first if(min_val >= first) else min_val
            except:
                pass

        return min_val, max_val

    def get_variables_min_max(self, postion):
        """
        """
        with open(self.file_path) as csv_file:
            reader = csv.reader(csv_file, delimiter='\t')
            return self.__get_min_max(reader, postion)
    

    def get_temporal(self, time_position=0, time_units='hours', scale_factor=1.0, offset=0,
                     date_format = '%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The NetCDF variable we need to target
        :param units_variable: The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """
        reg_ex = r'.*(\d{8})_(\d{4})'
        extract_date_time = re.search(reg_ex, self.file_path).group
        
        group = (time_units, "%s %s" %(extract_date_time(1), extract_date_time(2)))
        min_data, max_data = self.get_variables_min_max(time_position)
        args_min = {group[0]: (scale_factor*min_data) + offset}
        args_max = {group[0]: (scale_factor * max_data) + offset}
        start_date = self.get_updated_date(group[1], **args_min)
        stop_date = self.get_updated_date(group[1], **args_max)
        #print(date_format)
        if date_format:
            start_date, stop_date = start_date.strftime(date_format), stop_date.strftime(date_format)
        return start_date, stop_date

    def get_wnes_geometry(self, lon_position=16, lat_position=15, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a netCDF file
        :param nc_data: netCDF data
        :param variable_lon_key:  The NetCDF variable we need to target
        :param variable_lat_key:  The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """

        south, north = [round((x * scale_factor) + offset, 3) for x in self.get_variables_min_max(lon_position)]
        west, east = [round((x * scale_factor) + offset, 3) for x in self.get_variables_min_max(lat_position)]

        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]
    
    def get_metadata(self, ds_short_name, time_position=0, time_units="hours", lon_postion=15, date_format='%Y-%m-%dT%H:%M:%SZ',
                     lat_postion=16, format='CSV', version='1'):
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
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = self.get_temporal(time_position=time_position, time_units=time_units,
                                                                              date_format=date_format)

        gemetry_list =self.get_wnes_geometry(lon_position=lon_postion, lat_position=lat_postion)

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data

if __name__ == '__main__':
    print("Dependencies works")

    file_path = "/home/amarouane/Downloads/lpvex_Kumpula_20101012_0834.tsv"
    exnet = ExtractCSVMetadata(file_path)
    metada = exnet.get_metadata("test")
    print(metada)