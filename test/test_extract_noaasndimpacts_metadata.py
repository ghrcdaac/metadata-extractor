from os import path
import json
from unittest import TestCase
from granule_metadata_extractor.processing.process_noaasndimpacts import ExtractNoaasndimpactsMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML

#host=thor,env=ops,project=IMPACTS,ds=noaasndimpacts,inv=inventory,file=IMPACTS_sounding_20200101_000000_ALB.nc,path=20200101/IMPACTS_sounding_20200101_000000_ALB.nc,size=37749,start=2020-01-01T00:00:00Z,end=2020-01-01T11:59:59Z,browse=N,checksum=f9184eb48eb94e3fecd217ae22394a86eaf3abd5,NLat=42.701899999999995,SLat=42.6819,WLon=-73.8422,ELon=-73.8222,format=netCDF-4

class TestProcess(TestCase):
    """
    Test processing netCDF-4.
    This will test if this dataset's metadata will be extracted correctly
    """
    granule_name = "IMPACTS_sounding_20200101_000000_ALB.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")  
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    locating_time = '201009151537'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractNoaasndimpactsMetadata(input_file)
    expected_metadata = {'ShortName': 'noaasndimpacts',
    'GranuleUR': granule_name,
     'VersionId': '0.1', 'DataFormat': 'netCDF-4',
      }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date
        
        
        self.assertEqual(start_date, "2020-01-01T00:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_dataset.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        
        self.assertEqual(stop_date, "2020-01-01T11:59:59Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = round(self.process_dataset.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.04)


    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(wnes[index], 3))

   #NLat=42.701899999999995,SLat=42.6819,WLon=-73.8422,ELon=-73.8222
 
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '42.702')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-73.842')
    
    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '42.682')
    
    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-73.822')

    
    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """
    
        checksum = self.process_dataset.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'cb3713c772b24133eac67f1015212b2b')
    
    
    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """
        
        metadata = self.process_dataset.get_metadata(ds_short_name='noaasndimpacts',
         format='netCDF-4', version='0.1')
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
