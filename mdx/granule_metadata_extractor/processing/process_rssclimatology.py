from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import os


class ExtractRssClimatologyMetadata(ExtractNetCDFMetadata):
    """
    A class to extract Npolimpacts
    """

    def __init__(self, file_path):
        super().__init__(file_path)
        self.file_path = file_path


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
        basename = os.path.basename(self.file_path)
        # To make it fail if the regex didn't match
        start_date = stop_date = None

        if re.match(r"^.*(\d{4})(\d{2})\.nc[3-4]\.nc", basename):
            yyyy, mm = [re.match(r"^.*(\d{4})(\d{2})\.nc[3-4]\.nc", basename).group(ele) for ele in [1, 2]]
            start_date = datetime(year=int(yyyy), month=int(mm), day=1)
            stop_date = start_date + relativedelta(months=1, seconds=-1)

        if re.match(r"^.*(\d{4})(\d{2})_(\d{4})(\d{2})_.*\.nc[3-4]\.nc", basename):
            start_yyyy, start_mm, end_yyyy, end_mm = [re.match(r"^.*(\d{4})(\d{2})_(\d{4})(\d{2})_.*\.nc[3-4]\.nc",
                                                               basename).group(ele) for ele in range(1,5)]
            start_date = datetime(year=int(start_yyyy), month=int(start_mm), day=1)
            stop_date = datetime(year=int(end_yyyy), month=int(end_mm), day=1) + relativedelta(months=1, seconds=-1)
        if re.match(r"^.*(\d{4})_(\d{4})_.*\.nc[3-4]\.nc", basename):
            start_yyyy, end_yyyy = [re.match(r"^.*(\d{4})_(\d{4})_.*\.nc[3-4]\.nc",
                                             basename).group(ele) for ele in range(1, 3)]
            start_date = datetime(year=int(start_yyyy), month=1, day=1)
            stop_date = datetime(year=int(end_yyyy) + 1, month=1, day=1) + relativedelta(seconds=-1)
        if date_format:
            start_date, stop_date = [format_date.strftime(date_format) for format_date in [start_date, stop_date]]
        return start_date, stop_date




