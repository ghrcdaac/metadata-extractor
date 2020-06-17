import os
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from hashlib import md5
from abc import ABC, abstractmethod

class MetadataExtractor(ABC):
    """
    Class to extract netCDF metadata
    """
    def __init__(self, file_path):
        self.file_path = file_path
        super().__init__()



    @abstractmethod
    def get_variables_min_max(self, variable_key):
        """
        Abstract 
        """
        pass

    def convert_360_to_180(self, coord):
        """
        Convert the coordinate from 360to 180
        :param coord: the coordinate
        :return: coord in [-180, 180]
        """
        return round(((coord+180) % 360)-180, 3) if coord > 180 else coord

    @abstractmethod
    def get_wnes_geometry(self):
        """
        Abstract method to get bonding box
        """

        pass

    def get_updated_date(self, date_time_origin, **args):
        """

        :param date_time_origin:
        :param args:
        :return:
        """
        return parse(date_time_origin) + relativedelta(**args)

    def get_checksum(self):
        """
        :return: MD5 checksum
        """
        with open(self.file_path, 'rb') as file:
            return md5(file.read()).hexdigest()

    def get_file_size_bytes(self):
        """
        :return: size in bytes
        """
        return os.path.getsize(self.file_path)

    def get_file_size_megabytes(self):
        """

        :return: size in bytes
        """
        return 1E-6 * self.get_file_size_bytes()

    @abstractmethod
    def get_temporal(self):
        """
        Abstract method to compute temporal 
        """
        pass

    @abstractmethod
    def get_metadata(self):
        """
        Abstract extracting metadata
        """
        pass



if __name__ == '__main__':
    print("Dependencies works")

   
