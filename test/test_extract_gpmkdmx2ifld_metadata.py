from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_gpmkdmx2ifld import ExtractGpmkdmx2ifldMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#"Level2_KDMX_20130618_0148.ar2v": {"temporal": ["2013-06-18T01:48:00Z", "2013-06-18T01:57:59Z"], "wnes_geometry": ["-99.26", "45.87", "-88.18", "37.59"], "SizeMBDataGranule": "24.05", "checksum": "a4d9faf34539841078a3a66e4f9832ea", "format": "Binary"}

class TestProcessGpmkdmx2ifld(TestCase):
    """
    Test processing dataset metadata.
    This will test if metadata will be extracted correctly
    """
    granule_name = "Level2_KDMX_20130618_0148.ar2v"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractGpmkdmx2ifldMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'gpmkdmx2ifld')
    expected_metadata = {'ShortName': 'gpmkdmx2ifld',
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
        self.assertEqual(start_date, "2013-06-18T01:48:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_dataset.get_temporal_lookup(self.granule_name)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        self.assertEqual(stop_date, "2013-06-18T01:57:59Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 24.05)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_gpm = self.process_dataset
        wnes = process_gpm.get_wnes_geometry_lookup(self.granule_name)
        return str(round(float(wnes[index]), 3))

    #"wnes_geometry": ["-99.26", "45.87", "-88.18", "37.59"]
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '45.87')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-99.26')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '37.59')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-88.18')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'a4d9faf34539841078a3a66e4f9832ea')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of gpmkdmx2ifld 
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='gpmkdmx2ifld',
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