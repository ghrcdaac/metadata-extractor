from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_navdc8cpex import ExtractNavdc8cpexMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for sample file:
#host=thor,env=ops,project=CPEX,ds=navdc8cpex,inv=inventory,file=CPEX_Hskping_DC8_20170525_R0.ict,path=20170525/CPEX_Hskping_DC8_20170525_R0.ict,size=3648971,start=2017-05-25T16:22:36Z,end=2017-05-25T21:06:38Z,browse=N,checksum=85b81b5eaf65c3f2138356921ddea64917a21de1,NLat=34.9342918,SLat=25.9311676,WLon=-118.1459427,ELon=-79.9773788,format=ASCII
class TestProcessNavdc8cpex(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "CPEX_Hskping_DC8_20170525_R0.ict"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractNavdc8cpexMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'navdc8cpex')
    expected_metadata = {'ShortName': 'navdc8cpex',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'ASCII',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2017-05-25T16:22:36Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2017-05-25T21:06:38Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 3.65)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=34.9342918,SLat=25.9311676,WLon=-118.1459427,ELon=-79.9773788
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '34.934')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-118.146')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '25.931')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-79.977')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '94faeb6e765e742cca5d7ff57d003830')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='navdc8cpex',
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
