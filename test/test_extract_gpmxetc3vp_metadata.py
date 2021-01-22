from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_gpmxetc3vp import ExtractGpmxetc3vpMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML


# prem metadata for sample file: host=thor,env=ops,project=C3VP,ds=gpmxetc3vp,inv=inventory,
# file=C3VP_XET.csv,path=C3VP_XET.csv,size=37531713, start=2006-10-04T00:00:00Z,
# end=2007-03-31T23:59:00Z,browse=N, checksum=8780619d073c1d978e7c7f7665c5e6d1b9a86f87,
# NLat=44.239999999999995, SLat=44.22,WLon=-79.79,ELon=-79.77,format=ASCII-csv 

class TestProcessGpmxetc3vp(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "C3VP_XET_test.csv"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractGpmxetc3vpMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name='gpmxetc3vp')
    expected_metadata = {'ShortName': 'gpmxetc3vp',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'ASCII-csv',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2006-10-04T00:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2006-10-04T00:03:00Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.0)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '44.24')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-79.79')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '44.22')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-79.77')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'ddd88ee4ca6f0afbfe03625f4ea4220a')

    def test_9_generate_metadata(self):
        """
        Test generating metadata 
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='gpmxetc3vp',
                                                     format='ASCII-csv', version='1')
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
