from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_sbuceilimpacts import ExtractSbuceilimpactsMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#host=thor,env=ops,project=IMPACTS,ds=sbuceilimpacts,inv=inventory,file=IMPACTS_SBU_ceilo_20200102_2356_chm15k_RT.nc,path=CHM15k_RadarTruck/20200102/IMPACTS_SBU_ceilo_20200102_2356_chm15k_RT.nc,size=66892,start=2020-01-02T23:56:51Z,end=2020-01-02T23:59:51Z,browse=N,checksum=0a61a107623e94cd87cb198322629edd124577c8,NLat=40.89726948124859,SLat=40.896730518751404,WLon=-73.12726948124859,ELon=-73.1267305187514,format=netCDF-3
class TestProcessSbuceilimpacts(TestCase):
    """
    Test processing sbuceilimpacts.
    This will test if sbuceilimpacts metadata will be extracted correctly
    """
    granule_name = "IMPACTS_SBU_ceilo_20200102_2356_chm15k_RT.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_sbuceilimpacts = ExtractSbuceilimpactsMetadata(input_file)
    md = process_sbuceilimpacts.get_metadata(ds_short_name= 'sbuceilimpacts')
    expected_metadata = {'ShortName': 'sbuceilimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'netCDF-3',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        # start_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[0]
        start_date = self.process_sbuceilimpacts.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2020-01-02T23:56:51Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        # stop_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[1]
        stop_date = self.process_sbuceilimpacts.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-01-02T23:59:51Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        # file_size = round(self.process_goesrpltavirisng.get_file_size_megabytes(), 2)
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.07)

    #NLat=40.89726948124859,SLat=40.896730518751404,WLon=-73.12726948124859,ELon=-73.1267305187514
    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_sbuceil = self.process_sbuceilimpacts
        # wnes = process_geos.get_wnes_geometry()
        wnes = process_sbuceil.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '40.897')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-73.127')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '40.897')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-73.127')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'bcb1c7ac2a3a11b5f80c973f63dc1396')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltavirisng
        :return: metadata object 
        """

        metadata = self.process_sbuceilimpacts.get_metadata(ds_short_name='sbuceilimpacts',
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
