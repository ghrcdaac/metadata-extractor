from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_p3metnavimpacts import ExtractP3metnavimpactsMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML

#host=thor,env=ops,project=IMPACTS,ds=p3metnavimpacts,inv=inventory,file=IMPACTS_MetNav_P3B_20200220_R0.ict,path=IMPACTS_MetNav_P3B_20200220_R0.ict,size=4378032,start=2020-02-20T19:39:12Z,end=2020-02-21T01:12:20Z,browse=N,checksum=dfba63ac0a1451f12b13bf1f2ec56eb96c931454,NLat=37.9763709,SLat=33.2614038,WLon=-79.8774242,ELon=-75.4506778,format=ASCII-ict

class TestProcessP3metnavimpacts(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "IMPACTS_MetNav_P3B_20200220_R0.ict"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractP3metnavimpactsMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'p3metnavimpacts')
    expected_metadata = {'ShortName': 'p3metnavimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'ASCII-ict',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2020-02-20T19:39:12Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-02-21T01:12:20Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 4.38)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_p3metnav = self.process_dataset
        wnes = process_p3metnav.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))


    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '37.976')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-79.877')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '33.261')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-75.451')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '2990383a904a9df420844ec535d0bc83')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of sbuplimpacts
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='p3metnavimpacts',
                                                     format='ASCII-ict', version='1')
        for key in self.expected_metadata.keys():
            self.assertEqual(metadata[key], self.expected_metadata[key])

    def test_a1_generate_echo10(self):
        """
        Test generate the echo 10 in tmp folder
        """
        self.expected_metadata['OnlineAccessURL'] = "http://localhost.com"
        echo10xml = GenerateEcho10XML(self.expected_metadata)
        echo10xml.generate_echo10_xml_file()
        self.assertTrue(path.exists(f'/tmp/{self.granule_name}.cmr.xml'))

