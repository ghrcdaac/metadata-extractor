from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_aces1efm import ExtractAces1EfmMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson


class TestProcessAces1Efm(TestCase):
    """
    Test processing Aces1Efm.
    This will test if Aces1Efm metadata will be extracted correctly
    """
    granule_name = "aces1efm_2002.218_v2.50.tar"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_aces1 = ExtractAces1EfmMetadata(input_file)
    expected_metadata = {'ShortName': 'aces1efm',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'Binary',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        self.process_aces1.get_variables_min_max(self.granule_name)
        start_date = self.process_aces1.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2002-08-06T00:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        self.process_aces1.get_variables_min_max(self.granule_name)
        stop_date = self.process_aces1.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2002-08-06T23:59:59Z")

    def test_3_get_file_size(self):
        """
        Test getting the correct file size
        :return:
        """
        file_size = round(self.process_aces1.get_file_size_megabytes(), 2)
        print(file_size)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.07)

    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        # """
        process_geos = self.process_aces1
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
        """29.864
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
        checksum = self.process_aces1.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '81e3455b43c6d5e7850fb3be20008f24')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of aces1efm
        :return: metadata object
        """
        metadata = self.process_aces1.get_metadata(ds_short_name='aces1efm',
                                                       format='Binary', version='1')
        # print(self.expected_metadata.keys())
        for key in self.expected_metadata.keys():
            self.assertEqual(metadata[key], self.expected_metadata[key])

    def test_a1_generate_umm_json(self):
        """
        Test generate the umm json in tmp folder
        """
        self.expected_metadata['OnlineAccessURL'] = "http://localhost.com"
        umm_json = GenerateUmmGJson(self.expected_metadata)
        umm_json.generate_umm_json_file()
        self.assertTrue(path.exists(f'/tmp/{self.granule_name}.cmr.json'))
