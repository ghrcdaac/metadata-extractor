from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_gpmwacrc3vp import ExtractGpmwacrc3vpMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML

#prem metadata for sample file:
#host=thor,env=ops,project=C3VP,ds=gpmwacrc3vp,inv=inventory,file=c3vp_wacr_20070219_172850_v1.nc,path=c3vp_wacr_20070219_172850_v1.nc,size=3466516,start=2007-02-19T17:28:50Z,end=2007-02-19T19:55:33Z,browse=Y,checksum=21e5e092224713e388cd3508677caf36cfd8498c,NLat=44.23076629638672,SLat=44.230682373046875,WLon=-79.78286743164062,ELon=-79.78277587890625,format=netCDF-3

class TestProcessGpmwacrc3vp(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "c3vp_wacr_20070219_172850_v1.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractGpmwacrc3vpMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'gpmwacrc3vp')
    expected_metadata = {'ShortName': 'gpmwacrc3vp',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'netCDF-3',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2007-02-19T17:28:50Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2007-02-19T19:55:33Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 3.47)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=44.23076629638672,SLat=44.230682373046875,WLon=-79.78286743164062,ELon=-79.78277587890625

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '44.231')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-79.783')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '44.231')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-79.783')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'c2362670aeeb97e4361c4d9dc1258b68')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='gpmwacrc3vp',
                                                     format='netCDF-3', version='1')
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

