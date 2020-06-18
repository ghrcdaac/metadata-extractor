from os import path
from unittest import TestCase
import pyart


class TestProcessGeoER(TestCase):
    granule_name = "IMPACTS_nexrad_20200101_235815_kcle.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    expected_list = [45.543064, 37.28388, -76.34954, -87.37019]

    def get_nsew(self):
        """"
        Extract the north, south, east, west coordinates from the input file
        return: list of  [north, south, east, west]
        """
        radar = pyart.io.read_cfradial(self.input_file)
        lat = radar.gate_latitude['data'][:]
        lon = radar.gate_longitude['data'][:]
        return [lat.max(), lat.min(), lon.max(), lon.min()]

    def test_pyart_get_nsew(self):
        # north, south, east, west coordinates
        nsew = self.get_nsew()
        self.assertEqual(self.expected_list, nsew)
