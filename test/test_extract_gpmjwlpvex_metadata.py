from os import path
import json
from unittest import TestCase
from granule_metadata_extractor.processing.process_gpmjwlpvex import ExtractGpmjwlpvexMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML



class TestProcessGpmjw(TestCase):
    """
    Test processing Geosr.
    This will test if goesrpltcrs metadata will be extracted correctly
    """
    granule_name = "jw_lpvex_Jarvenpaa_20101103-0000.txt"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")  
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    locating_time = '201009151537'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_goesrpltcrs = ExtractGpmjwlpvexMetadata(input_file)
    expected_metadata = {'ShortName': 'gpmjwlpvex',
    'GranuleUR': granule_name,
     'VersionId': '0.1', 'DataFormat': 'ASCII',
      }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_goesrpltcrs.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date
        
        
        self.assertEqual(start_date, "2010-11-03T00:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_goesrpltcrs.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        
        self.assertEqual(stop_date, "2010-11-03T02:59:00Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = round(self.process_goesrpltcrs.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.02)


    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_goesrpltcrs
        wnes = process_geos.get_wnes_geometry()
        return str(round(wnes[index], 3))

    
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '60.495')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '25.072')
    
    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '60.475')
    
    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '25.092')

    
    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """
    
        checksum = self.process_goesrpltcrs.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'b57d48aa760791d74c1045949673bfb9')
    
    
    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltcrs
        :return: metadata object 
        """
        
        metadata = self.process_goesrpltcrs.get_metadata(ds_short_name='gpmjwlpvex',
         format='ASCII', version='0.1')
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