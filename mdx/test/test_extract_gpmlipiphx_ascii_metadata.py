from os import path
import json
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_gpmlipiphx_ascii import ExtractGpmlipiphxASCIIMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#"14121": {"start": "2014-05-01T18:00:29Z", "end": "2014-05-01T21:49:46Z", "NLat": 36.4756, "SLat": 34.1076, "ELon": -117.9568, "WLon": -121.853}
#host=thor,env=ops,project=IPHEx,ds=gpmlipiphx,inv=inventory,file=iphex_lip_14121.txt,path=iphex_lip_14121.txt,size=9442575,start=2014-05-01T18:00:29Z,end=2014-05-01T21:49:46Z,browse=Y,checksum=f9184b16d127af44759f12ec4fb108fceb77351a,NLat=36.4756,SLat=34.1076,WLon=-121.853,ELon=-117.9568,format=ASCII

class TestProcess(TestCase):
    """
    Test processing ASCII.
    This will test if this dataset's ASCII metadata will be extracted correctly
    """
    granule_name = "iphex_lip_14121.txt"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")  
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    locating_time = '201009151537'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractGpmlipiphxASCIIMetadata(input_file)
    expected_metadata = {'ShortName': 'gpmlipiphx',
    'GranuleUR': granule_name,
     'VersionId': '0.1', 'DataFormat': 'ASCII',
      }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date
        
        
        self.assertEqual(start_date, "2014-05-01T18:00:29Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_dataset.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        
        self.assertEqual(stop_date, "2014-05-01T21:49:46Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = round(self.process_dataset.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.0)


    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(wnes[index], 3))
#"NLat": 36.4756, "SLat": 34.1076, "ELon": -117.9568, "WLon": -121.853
    
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '36.476')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-121.853')
    
    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '34.108')
    
    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-117.957')

    
    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """
    
        checksum = self.process_dataset.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'eeccf0993b4f62ae956ab89b50bf0862')
    
    
    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """
        
        metadata = self.process_dataset.get_metadata(ds_short_name='gpmlipiphx',
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
