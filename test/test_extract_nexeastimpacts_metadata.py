from os import path
import json
from unittest import TestCase
from granule_metadata_extractor.processing.process_nexeastimpacts import ExtractNexeastimpactsMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML

#host=thor,env=ops,project=IMPACTS,ds=nexeastimpacts,inv=inventory,file=IMPACTS_nexrad_20200101_235800_mosaic_east.nc,path=20200101/IMPACTS_nexrad_20200101_235800_mosaic_east.nc,size=14373704,start=2020-01-01T23:40:46Z,end=2020-01-01T23:57:44Z,browse=N,checksum=e0c4d510a3b74a00ffee0d3175bbcd9c102e03ac,NLat=46.474998474121094,SLat=32.5,WLon=-85.0,ELon=-67.5250015258789,format=netCDF-4

class TestProcess(TestCase):
    """
    Test processing netCDF-4.
    This will test if this dataset's metadata will be extracted correctly
    """
    granule_name = "IMPACTS_nexrad_20200101_235800_mosaic_east.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")  
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    locating_time = '201009151537'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractNexeastimpactsMetadata(input_file)
    expected_metadata = {'ShortName': 'nexeastimpacts',
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
        
        
        self.assertEqual(start_date, "2020-01-01T23:40:46Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_dataset.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        
        self.assertEqual(stop_date, "2020-01-01T23:57:44Z")

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

    #NLat=46.474998474121094,SLat=32.5,WLon=-85.0,ELon=-67.5250015258789 

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '46.475')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-85.0')
    
    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '32.5')
    
    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-67.525')

    
    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """
    
        checksum = self.process_dataset.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '889ae416c69060c66d4773828058537e')
    
    
    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """
        
        metadata = self.process_dataset.get_metadata(ds_short_name='nexeastimpacts',
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
