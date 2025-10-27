from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_exradepoch import ExtractExradepochMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for sample file:
#host=thor,env=ops,project=EPOCH,ds=exradepoch,inv=inventory,file=EPOCH_2017_EXRAD_NADIR_Level1B_V03__20170809T041528_to_20170809T194010_001.h5,path=EPOCH_2017_EXRAD_NADIR_Level1B_V03__20170809T041528_to_20170809T194010_001.h5,size=559608177,start=2017-08-09T04:15:28Z,end=2017-08-09T19:40:10Z,browse=N,checksum=78b06eecbe577d620ce5bd22d9ee277e74344150,NLat=32.20483263917093,SLat=18.514284259086928,WLon=-110.35154531957646,ELon=-90.56559982523692,format=HDF-5

class TestProcessExradepoch(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "EPOCH_2017_EXRAD_NADIR_Level1B_V03__20170809T041528_to_20170809T194010_001.h5"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractExradepochMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'exradepoch')
    expected_metadata = {'ShortName': 'exradepoch',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'HDF-5',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2017-08-09T04:15:28Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2017-08-09T19:40:10Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size,0.88 )

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=32.20483263917093,SLat=18.514284259086928,WLon=-110.35154531957646,ELon=-90.56559982523692

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '32.205')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-110.352')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '18.514')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-90.566')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '9033bdc565f1bfe3b5fd993528a3ff20')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='exradepoch',
                                                     format='HDF-5', version='1')
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
