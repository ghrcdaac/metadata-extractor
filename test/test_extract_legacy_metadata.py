from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_legacy import ExtractLegacyMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML
import re


class TestProcessLegacy(TestCase):
    """
    Test processing Legacy.
    This will test if Legacy metadata will be extracted correctly
    """
    granule_name = "aces1trig_2002.242_v2.50.tar"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_Legacy = ExtractLegacyMetadata(input_file)
    expected_metadata = {'ShortName': 'aces1trig',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'Binary',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_Legacy.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2002-08-30T00:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_Legacy.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2002-08-30T23:59:00Z")

    def test_3_get_file_size(self):
        """
        Test getting the correct file size
        :return:
        """
        # file_size = round(self.process_Legacy.get_file_size_megabytes(), 2)
        file_size = round(self.process_Legacy.file_size, 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 593.24)

    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_Legacy
        wnes = process_geos.get_wnes_geometry()
        return str(round(wnes[index], 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '26.0')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-85.0')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '23.0')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-81.0')

    def test_8_get_checksum(self):
        """
        Test getting the checksum of the input file
        :return: the MD5 string
        """
        # checksum = self.process_Legacy.get_checksum()
        checksum = self.process_Legacy.checksum
        self.expected_metadata['checksum'] = checksum
        # self.assertEqual(checksum, '68b67c53a899df34537118f34a58ecb0')
        # Because we are randomly generating a checksum for granules which don't have one, we are instead
        # unit testing for the expected checksum pattern rather than a specific checksum
        self.assertRegex(checksum, r'^[0-9a-fA-F]{32}$')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of Legacy
        :return: metadata object
        """

        metadata = self.process_Legacy.get_metadata(ds_short_name='aces1trig',
                                                       format='Binary', version='1')
        for key in self.expected_metadata.keys():
            self.assertEqual(metadata[key], self.expected_metadata[key])

    def test_a1_generate_echo10(self):
        """
        Test generate the echo 10 in tmp folder
        :return: None
        """
        self.expected_metadata['OnlineAccessURL'] = "http://localhost.com"
        echo10xml = GenerateEcho10XML(self.expected_metadata)
        echo10xml.generate_echo10_xml_file()
        self.assertTrue(path.exists(f'/tmp/{self.granule_name}.cmr.xml'))
