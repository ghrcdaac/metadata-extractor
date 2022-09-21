from os import path
import json
from unittest import TestCase
from granule_metadata_extractor.src.extract_netcdf_metadata import ExtractNetCDFMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson



class TestProcessRSS(TestCase):
    """
    Test processing Geosr.
    This will test if goesrpltcrs metadata will be extracted correctly
    """
    granule_name = "f17_ssmis_201911v7.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'void'
    lon_var_key = 'longitude'
    lat_var_key = 'latitude'
    time_units = 'units'
    process_goesrpltcrs = ExtractNetCDFMetadata(input_file)
    expected_metadata = {'ShortName': 'rssmif16w',
    'GranuleUR': granule_name,
     'VersionId': '0.1', 'DataFormat': 'NetCDF-4',
      }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_goesrpltcrs.get_temporal(time_variable_key=self.time_var_key)[0]
        self.expected_metadata['BeginningDateTime'] = start_date
        
        
        self.assertEqual(start_date, "2019-11-01T00:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_goesrpltcrs.get_temporal(time_variable_key=self.time_var_key)[1]
        self.expected_metadata['EndingDateTime'] = stop_date
        
        self.assertEqual(stop_date, "2019-11-30T23:59:59Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = round(self.process_goesrpltcrs.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 1.21)


    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        
        variable_lon_key = self.lon_var_key
        variable_lat_key = self.lat_var_key
        process_geos = self.process_goesrpltcrs
        wnes = process_geos.get_wnes_geometry(variable_lon_key=variable_lon_key, variable_lat_key=variable_lat_key)
        return str(round(wnes[index], 3))


    
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '89.875')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '0.125')
    
    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '-89.875')
    
    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-0.125')

    
    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """
    
        checksum = self.process_goesrpltcrs.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '37778e429496c6f1649bec4770e3e58f')
    
    
    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltcrs
        :return: metadata object 
        """
        
        metadata = self.process_goesrpltcrs.get_metadata(ds_short_name='rssmif16w',
        time_variable_key=self.time_var_key,lon_variable_key=self.lon_var_key,
        lat_variable_key=self.lat_var_key,time_units=self.time_units, format='NetCDF-4', version='0.1')
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
