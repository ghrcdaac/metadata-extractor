from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_tammsimpacts import ExtractTammsimpactsMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML


class TestProcessTammsimpacts(TestCase):
    """
    Test processing Tammsimpacts.
    This will test if tammsimpacts metadata will be extracted correctly
    """
    granule_name = "IMPACTS_TAMMS_P3_20200205_R0.ict"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_tammsimpacts = ExtractTammsimpactsMetadata(input_file)
    expected_metadata = {'ShortName': 'tammsimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'ASCII-ict',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_tammsimpacts.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2020-02-05T18:30:43Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_tammsimpacts.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-02-05T18:30:43Z")

    def test_3_get_file_size(self):
        """
        Test getting the correct file size
        :return:
        """
        file_size = round(self.process_tammsimpacts.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.0)

    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_tammsimpacts
        wnes = process_geos.get_wnes_geometry()
        return str(round(wnes[index], 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '37.924')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-75.444')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '37.924')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-75.444')

    def test_8_get_checksum(self):
        """
        Test getting the checksum of the input file
        :return: the MD5 string
        """

        checksum = self.process_tammsimpacts.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '012a23baa52f5222a0f719f8e099cb96')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of tammsimpacts
        :return: metadata object
        """

        metadata = self.process_tammsimpacts.get_metadata(ds_short_name='tammsimpacts',
                                                          format='ASCII-ict', version='1')
        # print(self.expected_metadata.keys())
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