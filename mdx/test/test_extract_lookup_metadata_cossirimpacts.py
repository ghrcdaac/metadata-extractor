from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_lookup import ExtractLookupMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

###prem metadata for sample file:
#impacts_cossir_20230226_along_track_v1.nc
#{'start': '2023-02-26T15:24:09Z', 'end': '2023-02-26T18:12:29Z', 'north': '32.33', 'south': '26.72', 'east': '-82.35', 'west': '-86.571', 'format': 'netCDF-4', 'sizeMB': 14.57}
class TestProcessLookup(TestCase):
    """
    Test lookup collection metadata extraction
    """
    granule_name = "impacts_cossir_20230226_along_track_v1.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_instance = ExtractLookupMetadata(input_file)
    md = process_instance.get_metadata(ds_short_name= 'cossirimpacts', format="Not Provided")
    expected_metadata = {'ShortName': 'cossirimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'netCDF-4',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.md['BeginningDateTime']
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2023-02-26T15:24:09Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.md['EndingDateTime']
        self.expected_metadata['EndingDateTime'] = stop_date
      
        self.assertEqual(stop_date, "2023-02-26T18:12:29Z")

    def str_to_num(self, s):
        """
        Pars string to either int or float depending on value
        """
        try:
            return int(s)
        except ValueError:
            return float(s)

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = self.str_to_num(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0)

    #'north': '32.33', 'south': '26.72', 'east': '-82.35', 'west': '-86.571'
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.md['NorthBoundingCoordinate']
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '32.33')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.md['WestBoundingCoordinate']
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-86.571')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.md['SouthBoundingCoordinate']
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '26.72')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.md['EastBoundingCoordinate']
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-82.35')

    def test_8_generate_metadata(self):
        """
        Test generating metadata of legacy collection
        :return: metadata object 
        """
        metadata = self.process_instance.get_metadata(ds_short_name='cossirimpacts',
                                                     format='Not provided', version='1')
        for key in self.expected_metadata.keys():
            self.assertEqual(metadata[key], self.expected_metadata[key])

    def test_9_generate_umm_json(self):
        """
        Test generate the umm json in tmp folder
        """
        self.expected_metadata['OnlineAccessURL'] = "http://localhost.com"
        umm_json = GenerateUmmGJson(self.expected_metadata)
        umm_json.generate_umm_json_file()
        self.assertTrue(path.exists(f'/tmp/{self.granule_name}.cmr.json'))
