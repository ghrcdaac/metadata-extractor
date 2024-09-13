from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_lookup import ExtractLookupMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

##host=thor,env=ops,project=IMPACTS,ds=p3metnavimpacts,inv=inventory,file=IMPACTS_MetNav_P3B_20220128_R0.ict,path=IMPACTS_MetNav_P3B_20220128_R0.ict,size=1605713,start=2022-01-28T16:12:24Z,end=2022-01-28T18:09:11Z,browse=N,checksum=5a4de8f9b2fc61e86bff25e70b9bf9c9f8573218,NLat=39.8275736,SLat=37.4497608,WLon=-84.2645088,ELon=-75.443603,format=ASCII
class TestProcessLookup(TestCase):
    """
    Test lookup collection metadata extraction
    """
    granule_name = "IMPACTS_MetNav_P3B_20220128_R0.ict"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_instance = ExtractLookupMetadata(input_file)
    md = process_instance.get_metadata(ds_short_name= 'p3metnavimpacts', format="Not Provided")
    expected_metadata = {'ShortName': 'p3metnavimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'ASCII',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.md['BeginningDateTime']
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2022-01-28T16:12:24Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.md['EndingDateTime']
        self.expected_metadata['EndingDateTime'] = stop_date
      
        self.assertEqual(stop_date, "2022-01-28T18:09:11Z")

    def str_to_num(self, s):
        """
        Pars string to either int or float depending on value
        """
        try:
            return int(s)
        except ValueError:
            return float(s)

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = self.str_to_num(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0)

    #NLat=39.8275736,SLat=37.4497608,WLon=-84.2645088,ELon=-75.443603
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.md['NorthBoundingCoordinate']
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '39.828')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.md['WestBoundingCoordinate']
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-84.265')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.md['SouthBoundingCoordinate']
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '37.45')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.md['EastBoundingCoordinate']
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-75.444')

    def test_8_generate_metadata(self):
        """
        Test generating metadata of legacy collection
        :return: metadata object 
        """
        metadata = self.process_instance.get_metadata(ds_short_name='p3metnavimpacts',
                                                     format='Not provided', version='1')
        for key in self.expected_metadata.keys():
            self.assertEqual(metadata[key], self.expected_metadata[key])

    def test_9_generate_umm_json(self):
        """
        Test generate the umm json in tmp folder
        """
        self.expected_metadata['OnlineAccessURL'] = "http://localhost.com"
        umm_json = GenerateUmmGJson(self.expected_metadata)
        umm_json.generate_umm_json_file()
        self.assertTrue(path.exists(f'/tmp/{self.granule_name}.cmr.json'))
