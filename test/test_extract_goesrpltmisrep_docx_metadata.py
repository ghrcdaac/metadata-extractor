from os import path
import json
from unittest import TestCase
import granule_metadata_extractor.processing as mdx
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML

#host=thor,env=ops,project=GOES-R PLT,ds=goesrpltmisrep,inv=inventory,file=GOES-R_mission-scientist-report_20170429_0.docx,path=mission_scientist/GOES-R_mission-scientist-report_20170429_0.docx,size=16749,start=2017-03-13T00:00:00Z,end=2017-05-17T23:59:59Z,browse=N,checksum=5fc0453b80e76b2728642420e190af6a3d9abfc7,NLat=43.573,SLat=26.449,WLon=-124.625,ELon=-72.202,format=MS Word

class TestProcessGoesrpltmisrep(TestCase):
    """
    Test processing Goesrpltmisrep.
    This will test if Goesrpltmisrep metadata will be extracted correctly
    """
    granule_name = "GOES-R_mission-scientist-report_20170429_0.docx"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")  
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    locating_time = '201009151537'#?
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_goesrpltcrs = mdx.ExtractGoesrpltmisrepMetadata(input_file)
    expected_metadata = {'ShortName': 'goesrpltmisrep',
    'GranuleUR': granule_name,
     'VersionId': '0.1', 'DataFormat': 'MS Word',
      }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_goesrpltcrs.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date
        
        
        self.assertEqual(start_date, "2017-03-13T00:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_goesrpltcrs.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        
        self.assertEqual(stop_date, "2017-05-17T23:59:59Z")

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
        self.assertEqual(north, '43.573')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-124.625')
    
    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '26.449')
    
    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-72.202')

    
    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """
    
        checksum = self.process_goesrpltcrs.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'd47def865530d12f447a146a9e3c464a')
    
    
    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltcrs
        :return: metadata object 
        """
        
        metadata = self.process_goesrpltcrs.get_metadata(ds_short_name='goesrpltmisrep',
         format='MS Word', version='0.1')
        print(self.expected_metadata.keys())
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
