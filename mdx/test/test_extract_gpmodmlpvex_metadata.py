from os import path
import json
from unittest import TestCase
from granule_metadata_extractor.processing.process_gpmodmlpvex import ExtractGpmodmlpvexMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson



class TestProcessGpm(TestCase):
    """
    Test processing Geosr.
    This will test if goesrpltcrs metadata will be extracted correctly
    """
    granule_name = "lpvex_SHP_Aranda_ODM_u100915_08.txt"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")  
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    locating_time = '201009151537'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_goesrpltcrs = ExtractGpmodmlpvexMetadata(input_file)
    expected_metadata = {'ShortName': 'gpmodmlpvex',
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
        
        
        self.assertEqual(start_date, "2010-09-15T08:21:26Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_goesrpltcrs.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        
        self.assertEqual(stop_date, "2010-09-15T15:37:40Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = round(self.process_goesrpltcrs.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.35)


    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        
        variable_locating_time= self.locating_time
        process_geos = self.process_goesrpltcrs
        wnes = process_geos.get_wnes_geometry(variable_locating_time)
        return str(round(wnes[index], 3))

    
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '60.172')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '21.501')
    
    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '59.886')
    
    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '21.7')

    
    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """
    
        checksum = self.process_goesrpltcrs.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'ec0952498bbdb87ff16401dda7199475')
    
    
    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltcrs
        :return: metadata object 
        """
        
        metadata = self.process_goesrpltcrs.get_metadata(ds_short_name='gpmodmlpvex',
         format='ASCII', version='0.1')
        #print(self.expected_metadata.keys())
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
