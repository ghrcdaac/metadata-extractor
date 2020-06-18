from os import path
from unittest import TestCase
import pyart


class TestProcessGeoER(TestCase):
    granule_name = "IMPACTS_nexrad_20200101_235815_kcle.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    expected_list = [45.543, 37.284, -76.350, -87.370]

    def get_nsew(self):
        """"
        Extract the north, south, east, west coordinates from the input file
        return: list of  [north, south, east, west]
        """
        radar = pyart.io.read_cfradial(self.input_file)
        lat = radar.gate_latitude['data'][:]
        lon = radar.gate_longitude['data'][:]
        nsew = [lat.max(), lat.min(), lon.max(), lon.min()]
        return list(map(lambda x: format(x, '.3f'), nsew))



    def test_pyart_get_nsew(self):
        # north, south, east, west coordinates
        nsew = self.get_nsew()
        self.assertEqual(self.expected_list, nsew)
