from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_gpmkdvn2ifld import ExtractGpmkdvn2ifldMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#"Level2_KDVN_20130618_0059.ar2v": {"temporal": ["2013-06-18T00:59:00Z", "2013-06-18T01:08:59Z"], "wnes_geometry": ["-96.1", "45.75", "-85.06", "37.47"], "SizeMBDataGranule": "44.98", "checksum": "0fa0b1a38b8993a23a125fcb1c5e0760", "format": "Binary"}

class TestProcessGpmkdvn2ifld(TestCase):
    """
    Test processing dataset metadata.
    This will test if metadata will be extracted correctly
    """
    granule_name = "Level2_KDVN_20130618_0059.ar2v"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractGpmkdvn2ifldMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'gpmkdvn2ifld')
    expected_metadata = {'ShortName': 'gpmkdvn2ifld',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'Binary',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal_lookup(self.granule_name)[0]
        self.expected_metadata['BeginningDateTime'] = start_date
        self.assertEqual(start_date, "2013-06-18T00:59:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_dataset.get_temporal_lookup(self.granule_name)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        self.assertEqual(stop_date, "2013-06-18T01:08:59Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 44.98)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_gpm = self.process_dataset
        wnes = process_gpm.get_wnes_geometry_lookup(self.granule_name)
        return str(round(float(wnes[index]), 3))

    #"wnes_geometry": ["-96.1", "45.75", "-85.06", "37.47"]
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '45.75')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-96.1')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '37.47')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-85.06')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '0fa0b1a38b8993a23a125fcb1c5e0760')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of gpmkdvn2ifld 
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='gpmkdvn2ifld',
                                                     format='Binary', version='1')
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
