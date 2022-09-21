from ..src.extract_browse_metadata import ExtractBrowseMetadata
from datetime import datetime,  timedelta


class ExtractGpmpipicepopAVIMetadata(ExtractBrowseMetadata):
    """
    A class to extract Gpmpipicepop AVI
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
        self.get_variables_min_max(file_path)

    def get_variables_min_max(self, variable_key):
        """

        :param variable_key: The AVI filename key we need to target
        :return: list longitude coordinates
        """

        if "_002_" in variable_key:  # Site 1
            self.max_lat = self.lat2
            self.min_lat = self.lat2
            self.max_lon = self.lon2
            self.min_lon = self.lon2
        elif "_003_" in variable_key:  # Site 2
            self.max_lat = self.lat3
            self.min_lat = self.lat3
            self.max_lon = self.lon3
            self.min_lon = self.lon3

        rr = {}
        try:
            fname = variable_key.split('/')[-1]
            tkn = fname.split('_')
            dt_str = tkn[3]  # YYYYMMDD
            rr['start'] = datetime.strptime(dt_str, '%Y%m%d%H%M')
            rr['end'] = rr['start'] + timedelta(seconds=59)  # 1-minute increment

            if rr['start'] < self.minTime:
                self.minTime = rr['start']
            if rr['end'] > self.maxTime:
                self.maxTime = rr['end']
        except Exception as e:
            print(e)

        return

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a AVI file
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the AVI not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in [self.max_lat, self.min_lat, self.max_lon, self.min_lon]]
        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]

    def get_temporal(self, time_variable_key='time', units_variable='units',  scale_factor=1.0, offset=0,
                     date_format = '%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The AVI variable we need to target
        :param units_variable: The AVI variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the AVI not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """

        start_date = self.minTime.strftime(date_format)
        stop_date = self.maxTime.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, format='AVI', version='01'):
        """
        Extract Metadata
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
    print('Extracting Gpmpipicepop AVI Metadata')
