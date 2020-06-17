from os import path
from unittest import TestCase
import granule_metadata_extractor.processing as mdx

from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML


class TestProcesser2mir(TestCase):
    """
    Test processing er2mir.
    This will test if er2mir metadata will be extracted correctly
    """
    granule_name = "er2mir_1998_251.gif"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_er2mir = mdx.ExtractEr2mirMetadata(input_file)
    expected_metadata = {'ShortName': 'er2mir',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'GIF',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_er2mir.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "1998-09-08T20:31:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_er2mir.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "1998-09-08T20:51:00Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = round(self.process_er2mir.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.02)

    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_er2mir
        wnes = process_geos.get_wnes_geometry()
        return str(round(wnes[index], 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '29.017')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-81.946')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '27.428')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-81.493')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        checksum = self.process_er2mir.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '95554bbf1b05cbf29a9f9442e4a7deab')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltcrs
        :return: metadata object 
        """

        metadata = self.process_er2mir.get_metadata(ds_short_name='er2mir',
                                                    format='GIF', version='1')
        #print(self.expected_metadata.keys())
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
