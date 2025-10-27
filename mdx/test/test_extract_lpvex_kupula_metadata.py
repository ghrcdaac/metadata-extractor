from os import path
import json
from unittest import TestCase
from mdx.granule_metadata_extractor.src.extract_csv_metadata import ExtractCSVMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson



class TestProcessLpvex(TestCase):
    """
    Test processing Geosr.
    This will test if lpvex metadata will be extracted correctly
    """
    granule_name = "lpvex_Kumpula_20101012_0834.tsv"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_position =  0
    lon_position = 15
    lat_position = 16
    time_units = 'hours'
    regex = '.*'
    process_lpvex = ExtractCSVMetadata(input_file)
    expected_metadata = {'ShortName': 'lpvex',
    'GranuleUR': granule_name,
     'VersionId': '0.1', 'DataFormat': 'CSV',
      }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_lpvex.get_temporal(time_position=self.time_position, time_units=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date
        
        
        self.assertEqual(start_date, "2010-10-12T08:34:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_lpvex.get_temporal(time_position=self.time_position, time_units=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        
        self.assertEqual(stop_date, "2011-07-20T22:34:00Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = round(self.process_lpvex.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.68)


    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_lpvex
        wnes = process_geos.get_wnes_geometry(lon_position=self.lon_position, lat_position=self.lat_position)
        return str(round(wnes[index], 3))


    
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '26.96')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '58.8')
    
    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '24.96')
    
    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '60.2')

    
    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """
    
        checksum = self.process_lpvex.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '018a125cacbd749be8c25b5465aa7dc5')
    
    
    def test_9_generate_metadata(self):
        """
        Test generating metadata of lpvex
        :return: metadata object 
        """
        
        metadata = self.process_lpvex.get_metadata(ds_short_name='lpvex',
        time_position=self.time_position, time_units=self.time_units, lon_postion=self.lon_position, format='CSV', version='0.1')
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
