from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_rssclimatology import ExtractRssClimatologyMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson


class TestProcessRSSClimatology(TestCase):
    """
    Test processing Geosr.
    This will test if tcspecmwf metadata will be extracted correctly
    """
    granules_name = ["ws_v07r01_198801_202008_cumulative.nc4.nc", "ws_v07r01_202008.nc3.nc"]
    input_files = [path.join(path.dirname(__file__), f"fixtures/{granule_name}") for granule_name in granules_name]
    time_var_key = 'time'
    lon_var_key = 'longitude'
    lat_var_key = 'latitude'
    time_units = 'units'
    file_format = 'netCDF-3'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    md = []
    expected_metadata = []
    for idx, input_file in enumerate(input_files):
        process_rss = ExtractRssClimatologyMetadata(input_file)
        md.append(process_rss.get_metadata(ds_short_name= 'rss1windnv7r01', lon_variable_key=lon_var_key,
                     lat_variable_key=lat_var_key, time_units='units', format=file_format))
        expected_metadata.append({'ShortName': 'rss1windnv7r01',
                         'GranuleUR': granules_name[idx],
                         'VersionId': '01', 'DataFormat': file_format,
                         })

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        expected_start_date = ["1988-01-01T00:00:00Z", "2020-08-01T00:00:00Z"]
        [self.assertEqual(val['BeginningDateTime'], expected_start_date[ixd]) for ixd, val in enumerate(self.md)]

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        expected_stop_date = ['2020-08-31T23:59:59Z', '2020-08-31T23:59:59Z']
        [self.assertEqual(val['EndingDateTime'], expected_stop_date[ixd]) for ixd, val in enumerate(self.md)]

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        expected_values = ['0.59', '0.53']
        [self.assertEqual(val['SizeMBDataGranule'], expected_values[ixd]) for ixd, val in enumerate(self.md)]



    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        [self.assertEqual(val['NorthBoundingCoordinate'], '89.5') for val in self.md]

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        [self.assertEqual(val['WestBoundingCoordinate'], '0.5') for val in self.md]

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        [self.assertEqual(val['SouthBoundingCoordinate'], '-89.5') for val in self.md]

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        [self.assertEqual(val['EastBoundingCoordinate'], '-0.5') for val in self.md]

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        expected_values = ['6ce520795b9be7927e8cd1452b5cf161', '6c61e364821ebe365bfe2215b116e1d0']
        [self.assertEqual(val['checksum'], expected_values[ixd]) for ixd, val in enumerate(self.md)]

    def test_9_generate_metadata(self):
        """
        Test generating metadata of rss climatology
        :return: metadata object
        """
        [[
            self.assertEqual(self.md[idx][key], val[key]) for idx, val in enumerate(self.expected_metadata)

        ] for key in self.expected_metadata[0].keys()]

    def test_a1_generate_umm_json(self):
        """
        Test generate the umm json in tmp folder
        """
        for md in self.md:
            md['OnlineAccessURL'] = "http://localhost.com"
            GenerateUmmGJson(md).generate_umm_json_file()
            self.assertTrue(path.exists(f"/tmp/{md['GranuleUR']}.cmr.json"))
