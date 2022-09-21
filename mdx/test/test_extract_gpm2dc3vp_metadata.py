from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_gpm2dc3vp import ExtractGpm2dc3vpMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#host=thor,env=ops,project=C3VP,ds=gpm2dc3vp,inv=inventory,file=c3vp_2dvd_sn16_V06336_flakes_noRA.txt,path=c3vp_2dvd_sn16_V06336_flakes_noRA.txt,size=5470,start=2006-12-02T08:23:53Z,end=2006-12-02T12:55:19Z,browse=N,checksum=7b5cd3fdf68709e23b93a8bd57e9af88fccc5d56,NLat=44.243180555555554,SLat=44.22318055555556,WLon=-79.79058611111111,ELon=-79.7705861111111,format=ASCII

class TestProcessGpm2dc3vp(TestCase):
    """
    Test processing gpm2dc3vp ascii.
    This will test if gpm2dc3vp metadata will be extracted correctly
    """
    granule_name = "c3vp_2dvd_sn16_V06336_flakes_noRA.txt"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractGpm2dc3vpMetadata(input_file)
    expected_metadata = {'ShortName': 'gpm2dc3vp',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'ASCII',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal(units_variable=self.time_units)[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2006-12-02T08:23:53Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_dataset.get_temporal(units_variable=self.time_units)[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2006-12-02T12:55:19Z")

    def test_3_get_file_size(self):
        """
        Test getting the correct file size
        :return:
        """
        file_size = round(self.process_dataset.get_file_size_megabytes(), 2)
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.01)

    def get_wnes(self, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        wnes = self.process_dataset.get_wnes_geometry()
        return str(round(wnes[index], 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '44.243')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-79.791')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '44.223')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-79.771')

    def test_8_get_checksum(self):
        """
        Test getting the checksum of the input file
        :return: the MD5 string
        """

        checksum = self.process_dataset.get_checksum()
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '1a67612d08db3bc34a3af87755261420')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='gpm2dc3vp',
                                                             format='ASCII', version='1')
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
