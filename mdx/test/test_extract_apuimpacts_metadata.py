from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_apuimpacts import ExtractApuimpactsMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson


class TestProcessapuimpacts(TestCase):
    """
    Test processing apuimpacts.
    This will test if apuimpacts metadata will be extracted correctly
    """
    granule_name = "impacts_apu01_rainparameter_min.txt"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_apuimpacts = ExtractApuimpactsMetadata(input_file)
    expected_metadata = {'ShortName': 'apuimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'ASCII',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_apuimpacts.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2020-01-16T05:13:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_apuimpacts.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-02-14T01:02:00Z")

    def test_3_get_file_size(self):
        """
        Test getting the correct file size
        :return:
        """
        file_size = round(self.process_apuimpacts.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.49)

    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_apu = self.process_apuimpacts
        wnes = process_apu.get_wnes_geometry()
        return str(round(wnes[index], 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '37.939')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-75.484')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '37.919')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-75.464')

    def test_8_get_checksum(self):
        """
        Test getting the checksum of the input file
        :return: the MD5 string
        """

        checksum = self.process_apuimpacts.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '1dae0a553cd974036ee8af08333fd8c8')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of 2dimpacts
        :return: metadata object 
        """

        metadata = self.process_apuimpacts.get_metadata(ds_short_name='apuimpacts',
                                                       format='ASCII', version='1')
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
