from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_isslisv1 import ExtractIsslisv1Metadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#host=thor,env=ops,project=ISSLIS,ds=isslisv1,inv=inventory,file=ISS_LIS_SC_V1.0_20201015_NQC_21495.nc,path=ISS_LIS_SC_V1.0_20201015_NQC_21495.nc,size=1729377,start=2020-10-15T01:13:12Z,end=2020-10-15T02:46:04Z,browse=N,checksum=dfbd6f6cc5ad90bd2505d71e21b4946fd08a2ebe,NLat=51.87992858886719,SLat=-51.69941329956055,WLon=-15.21644401550293,ELon=-44.393836975097656,format=netCDF-4

class TestProcessIsslisv1(TestCase):
    """
    Test processing isslisv1.
    This will test if isslisv1 metadata will be extracted correctly
    """
    granule_name = "ISS_LIS_SC_V1.0_20201015_NQC_21495.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_isslisv1 = ExtractIsslisv1Metadata(input_file)
    # test for NQC nc file
    md = process_isslisv1.get_metadata(ds_short_name= 'isslis_v1_nqc')
    expected_metadata = {'ShortName': 'isslis_v1_nqc',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'netCDF-4',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_isslisv1.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2020-10-15T01:13:12Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_isslisv1.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-10-15T02:46:04Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        # file_size = round(self.process_goesrpltavirisng.get_file_size_megabytes(), 2)
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 1.73)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_isslis = self.process_isslisv1
        wnes = process_isslis.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '51.88')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-15.216')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '-51.699')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-44.394')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'eea2976598ce8f91207ad2ab434ac48a')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltavirisng
        :return: metadata object 
        """

        metadata = self.process_isslisv1.get_metadata(ds_short_name='isslis_v1_nqc',
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
