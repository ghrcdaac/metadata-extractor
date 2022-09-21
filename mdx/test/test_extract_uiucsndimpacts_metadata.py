from os import path
import json
from unittest import TestCase
from granule_metadata_extractor.processing.process_uiucsndimpacts import ExtractUiucsndimpactsMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#host=thor,env=ops,project=IMPACTS,ds=uiucsndimpacts,inv=inventory,file=IMPACTS_UIUC_Mobile_research_sounding_20200118_1600.nc,path=IMPACTS_UIUC_Mobile_research_sounding_20200118_1600.nc,size=39395,start=2020-01-18T16:00:00Z,end=2020-01-18T17:30:10Z,browse=N,checksum=1b6744b81b7e6a8e2c400c1f3ad907add6da9ffe,NLat=42.111999999999995,SLat=42.092,WLon=-76.4,ELon=-76.38,format=netCDF-4

class TestProcess(TestCase):
    """
    Test processing netCDF-4.
    This will test if this dataset's metadata will be extracted correctly
    """
    granule_name = "IMPACTS_UIUC_Mobile_research_sounding_20200118_1600.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")  
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    locating_time = '201009151537'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractUiucsndimpactsMetadata(input_file)
    expected_metadata = {'ShortName': 'uiucsndimpacts',
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
        
        
        self.assertEqual(start_date, "2020-01-18T16:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_dataset.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        
        self.assertEqual(stop_date, "2020-01-18T17:30:10Z")

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

#NLat=42.111999999999995,SLat=42.092,WLon=-76.4,ELon=-76.38,format=netCDF-4
    
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '42.112')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-76.4')
    
    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '42.092')
    
    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-76.38')

    
    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """
    
        checksum = self.process_dataset.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '92c477977c8e370918195fae65a1319b')
    
    
    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """
        
        metadata = self.process_dataset.get_metadata(ds_short_name='uiucsndimpacts',
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
