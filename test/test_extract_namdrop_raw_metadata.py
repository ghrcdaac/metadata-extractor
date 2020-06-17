from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_namdrop_raw import ExtractNamdrop_rawMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML



class TestProcessNamdrop_raw(TestCase):
    """
    Test processing Namdrop_raw.
    This will test if namdrop_raw metadata will be extracted correctly
    """
    granule_name = "NAMMA_DROP_20060912_155745_P.dat"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")  
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_namdrop_raw = ExtractNamdrop_rawMetadata(input_file)
    expected_metadata = {'ShortName': 'namdrop_raw',
    'GranuleUR': granule_name,
     'VersionId': '1', 'DataFormat': 'ASCII',
      }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_namdrop_raw.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date
        
        
        self.assertEqual(start_date, "2006-09-12T15:57:46Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_namdrop_raw.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        
        self.assertEqual(stop_date, "2006-09-12T16:09:20Z")

    def test_3_get_file_size(self):
        """
        Test getting the correct file size
        :return:
        """
        file_size = round(self.process_namdrop_raw.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.25)


    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_namdrop_raw
        wnes = process_geos.get_wnes_geometry()
        return str(round(wnes[index], 3))

    
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '10.71')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-23.99')
    
    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '10.698')
    
    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-23.949')

    
    def test_8_get_checksum(self):
        """
        Test getting the checksum of the input file
        :return: the MD5 string
        """
    
        checksum = self.process_namdrop_raw.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'fb264840b00b0187e471a265600730e7')
    
    
    def test_9_generate_metadata(self):
        """
        Test generating metadata of namdrop_raw
        :return: metadata object 
        """
        
        metadata = self.process_namdrop_raw.get_metadata(ds_short_name='namdrop_raw',
         format='ASCII', version='1')
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
