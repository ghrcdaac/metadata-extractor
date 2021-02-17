from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_isslisgv1 import ExtractIsslisgv1Metadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML

#prem metadata for sample file:
#host=thor,env=ops,project=NOT APPLICABLE,ds=isslisg_v1_fin,inv=inventory,file=ISS_LIS_BG_V1.0_20200109_FIN_17159.hdf,path=hdf/2020/0109/ISS_LIS_BG_V1.0_20200109_FIN_17159.hdf,size=246872,start=2020-01-09T23:53:56Z,end=2020-01-09T23:59:47Z,browse=N,checksum=09d9774a381803ece6ccec431ce55e804b7eb4a4,NLat=-48.34272003173828,SLat=-51.67616271972656,WLon=-139.0858612060547,ELon=-115.11003875732422,format=HDF-4

class TestProcessIsslisgv1(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "ISS_LIS_BG_V1.0_20200109_FIN_17159.hdf"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractIsslisgv1Metadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'isslisg_v1_fin')
    expected_metadata = {'ShortName': 'isslisg_v1_fin',
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

        self.assertEqual(start_date, "2020-01-09T23:53:56Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-01-09T23:59:47Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.25)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=-48.34272003173828,SLat=-51.67616271972656,WLon=-139.0858612060547,ELon=-115.11003875732422

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '-48.343')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-139.086')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '-51.676')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-115.11')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '6d04c6e2de5cbcff3fa902cba0de1e8c')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='isslisg_v1_fin',
                                                     format='HDF-4', version='1')
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

