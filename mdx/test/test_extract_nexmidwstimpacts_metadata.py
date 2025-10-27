from os import path
import json
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_nexmidwstimpacts import ExtractNexmidwstimpactsMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#host=thor,env=ops,project=IMPACTS,ds=nexmidwstimpacts,inv=inventory,file=IMPACTS_nexrad_20200101_160300_mosaic_midwest.nc,path=20200101/IMPACTS_nexrad_20200101_160300_mosaic_midwest.nc,size=8786699,start=2020-01-01T15:45:39Z,end=2020-01-01T16:02:59Z,browse=N,checksum=6230d5b1262f9b9a9780c9239c263c6b3dde80da,NLat=45.974998474121094,SLat=36.0,WLon=-93.0,ELon=-79.0250015258789,format=netCDF-4

class TestProcess(TestCase):
    """
    Test processing netCDF-4.
    This will test if this dataset's metadata will be extracted correctly
    """
    granule_name = "IMPACTS_nexrad_20200101_160300_mosaic_midwest.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")  
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    locating_time = '201009151537'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractNexmidwstimpactsMetadata(input_file)
    expected_metadata = {'ShortName': 'nexmidwstimpacts',
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
        
        
        self.assertEqual(start_date, "2020-01-01T15:45:39Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_dataset.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        
        self.assertEqual(stop_date, "2020-01-01T16:02:59Z")

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

    #NLat=45.974998474121094,SLat=36.0,WLon=-93.0,ELon=-79.0250015258789

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '45.975')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-93.0')
    
    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '36.0')
    
    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-79.025')

    
    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """
    
        checksum = self.process_dataset.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '2c2421f659ffaa1df2c93210393e76ad')
    
    
    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """
        
        metadata = self.process_dataset.get_metadata(ds_short_name='nexmidwstimpacts',
         format='netCDF-4', version='0.1')
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
