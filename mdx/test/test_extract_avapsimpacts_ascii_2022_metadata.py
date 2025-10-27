from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_avapsimpacts import ExtractAvapsimpactsMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#host=thor,env=ops,project=IMPACTS,ds=avapsimpacts,inv=inventory,file=IMPACTS_AVAPS_P3B_202202081429_R0.ict,path=ASCII/IMPACTS_AVAPS_P3B_202202081429_R0.ict,size=112001,start=2022-02-08T14:29:42Z,end=2022-02-08T14:34:22Z,browse=N,checksum=e17a28fccfa4f7c4c5d8c77b6f0192b2cb12f95b,NLat=42.8,SLat=42.78,WLon=-65.9,ELon=-65.88,format=ASCII
class TestProcessAvapsimpacts(TestCase):
    """
    Test processing Avapsimpacts.
    This will test if avapsimpacts metadata will be extracted correctly
    """
    granule_name = "IMPACTS_AVAPS_P3B_202202081429_R0.ict"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_avapsimpacts = ExtractAvapsimpactsMetadata(input_file)
    expected_metadata = {'ShortName': 'avapsimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'ASCII',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_avapsimpacts.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2022-02-08T14:29:42Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_avapsimpacts.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2022-02-08T14:34:22Z")

    def test_3_get_file_size(self):
        """
        Test getting the correct file size
        :return:
        """
        file_size = round(self.process_avapsimpacts.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.11)

    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_avapsimpacts
        wnes = process_geos.get_wnes_geometry()
        return str(round(wnes[index], 3))

    #NLat=42.8,SLat=42.78,WLon=-65.9,ELon=-65.88
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '42.8')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-65.9')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '42.78')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-65.88')

    def test_8_get_checksum(self):
        """
        Test getting the checksum of the input file
        :return: the MD5 string
        """

        checksum = self.process_avapsimpacts.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '65b2061a32b7d5ea89e51c011919746a')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of avapsimpacts
        :return: metadata object 
        """

        metadata = self.process_avapsimpacts.get_metadata(ds_short_name='avapsimpacts',
                                                          format='ASCII', version='1')
        # print(self.expected_metadata.keys())
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
