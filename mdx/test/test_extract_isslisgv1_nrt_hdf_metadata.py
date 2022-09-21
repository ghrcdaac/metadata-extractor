from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_isslisgv1 import ExtractIsslisgv1Metadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for sample file:
#host=thor,env=ops,project=NOT APPLICABLE,ds=isslisg_v1_nrt,inv=inventory,file=ISS_LIS_BG_V1.0_20201026_210255_NRT_21679.hdf,path=ISS_LIS_BG_V1.0_20201026_210255_NRT_21679.hdf,size=87128,start=2020-10-26T21:03:22Z,end=2020-10-26T21:04:59Z,browse=N,checksum=91bb52d7c8e9c46a96ca25913d40edf71e2e4a5f,NLat=29.853199005126953,SLat=28.25045394897461,WLon=90.20879364013672,ELon=91.89403533935547,format=HDF-4

class TestProcessIsslisgv1(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "ISS_LIS_BG_V1.0_20201026_210255_NRT_21679.hdf"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractIsslisgv1Metadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'isslisg_v1_nrt')
    expected_metadata = {'ShortName': 'isslisg_v1_nrt',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'HDF-4',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2020-10-26T21:03:22Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-10-26T21:04:59Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.09)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=29.853199005126953,SLat=28.25045394897461,WLon=90.20879364013672,ELon=91.89403533935547

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '29.853')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '90.209')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '28.25')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '91.894')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'c4863f984710505335b3daf8d6fdd0c7')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltavirisng
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='isslisg_v1_nrt',
                                                     format='HDF-4', version='1')
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
