from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_lislip import ExtractLislipMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#host=thor,env=ops,project=LIS,ds=lislip,inv=inventory,file=TRMM_LIS_SC.04.1_2002.277.27868.nc,path=nc/2002/1004/TRMM_LIS_SC.04.1_2002.277.27868.nc,size=78167,start=2002-10-05T00:00:03Z,end=2002-10-05T00:02:51Z,browse=Y,checksum=2c78e01822257cad101a77ae93329bb62feca96d,NLat=-34.828678131103516,SLat=-35.05946731567383,WLon=-146.67478942871094,ELon=-141.45120239257812,format=netCDF-4

# note: testing four isslis science files. Uncomment each set for testing
class TestProcessLislip(TestCase):
    """
    Test processing lislip for nc file
    This will test if isslisv1 metadata will be extracted correctly
    """
    granule_name = "TRMM_LIS_SC.04.1_2002.277.27868.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_instance = ExtractLislipMetadata(input_file)

    # test for lislip nc file
    md = process_instance.get_metadata(ds_short_name= 'lislip')
    expected_metadata = {'ShortName': 'lislip',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'netCDF-4',
                         }



    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_instance.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2002-10-05T00:00:03Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_instance.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date
      
        self.assertEqual(stop_date, "2002-10-05T00:02:51Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.08)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        wnes = self.process_instance.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '-34.829')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-146.675')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '-35.059')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-141.451')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '9a2f8ff589cbbea3c6aa473b203ac7b7')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltavirisng
        :return: metadata object 
        """
        metadata = self.process_instance.get_metadata(ds_short_name='lislip',
                                                     format='netCDF-4', version='1')

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
