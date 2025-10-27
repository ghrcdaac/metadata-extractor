from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_legacy import ExtractLegacyMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

class TestProcessLegacy(TestCase):
    """
    Test legacy collection metadata extraction
    """
    granule_name = "wrfout_d02_2011-05-21_000000.GMI_HF.ORBITAL.TESTBED_TEL-SPHE.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_instance = ExtractLegacyMetadata(input_file)
    md = process_instance.get_metadata(ds_short_name= 'gpmsimorbmc3e')
    expected_metadata = {'ShortName': 'gpmsimorbmc3e',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'Not provided',
                         }



    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_instance.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2011-05-21T00:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_instance.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date
      
        self.assertEqual(stop_date, "2011-05-21T00:59:59Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 1.11)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        wnes = self.process_instance.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '43.0')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-121.0')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '28.0')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-91.0')

    def test_8_get_checksum(self):
        """
        Test geting the checksom of the input file
        :return: the MD5 string
        """

        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        # Because checksum is randomly assigned rather than generated for 
        # legacy collections, we only care that value is valid checksum
        self.assertRegex(checksum, r'[0-9a-fA-F]{16}')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of legacy collection
        :return: metadata object 
        """
        metadata = self.process_instance.get_metadata(ds_short_name='gpmsimorbmc3e',
                                                     format='Not provided', version='1')
        
        # We don't need to check checksum as it is randomly assigned rather
        # than generated for legacy collections.
        self.expected_metadata.pop('checksum')
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
