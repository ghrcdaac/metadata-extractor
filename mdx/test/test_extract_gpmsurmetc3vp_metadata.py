from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_gpmsurmetc3vp import ExtractGpmsurmetc3vpMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for sample file:
#host=thor,env=ops,project=C3VP,ds=gpmsurmetc3vp,inv=inventory,file=c3vp_ClimateData_Winter_062005_072007.xls,path=c3vp_ClimateData_Winter_062005_072007.xls,size=223232,start=2005-11-01T13:00:00Z,end=2007-03-31T21:00:00Z,browse=N,checksum=4ea6b09b5921ca4251753f8e574c7d16143d5f07,NLat=44.33,SLat=44.129999999999995,WLon=-79.86999999999999,ELon=-79.67,format=MS Excel

class TestProcessGpmsurmetc3vp(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "c3vp_ClimateData_Winter_062005_072007.xls"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractGpmsurmetc3vpMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'gpmsurmetc3vp')
    expected_metadata = {'ShortName': 'gpmsurmetc3vp',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'MS Excel',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2005-11-01T13:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2007-03-31T21:00:00Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.22)

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
        self.assertEqual(north, '44.33')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-79.87')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '44.13')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-79.67')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '87a5efacd5422cce78eff58887b68b56')

    def test_9_generate_metadata(self):
        """
        Test generating metadata 
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='gpmsurmetc3vp',
                                                     format='MS Excel', version='1')
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
