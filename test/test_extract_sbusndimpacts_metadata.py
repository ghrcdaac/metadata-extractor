from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_sbusndimpacts import ExtractSbusndimpactsMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML

#prem metadata for this file:
#host=thor,env=ops,project=IMPACTS,ds=sbusndimpacts,inv=inventory,file=IMPACTS_sounding_20200119_004158_SBU_Mobile.nc,path=20200119/IMPACTS_sounding_20200119_004158_SBU_Mobile.nc,size=361800,start=2020-01-19T00:41:58Z,end=2020-01-19T01:56:47Z,browse=N,checksum=ac2883603af51e1f1c6cdcbc841964555f22f176,NLat=41.52920913696289,SLat=40.965030670166016,WLon=-73.02999114990234,ELon=-70.86920928955078,format=netCDF-3

class TestProcessSbusndimpacts(TestCase):
    """
    Test processing sbusndimpacts.
    This will test if sbusndimpacts metadata will be extracted correctly
    """
    granule_name = "IMPACTS_sounding_20200119_004158_SBU_Mobile.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_sbusndimpacts = ExtractSbusndimpactsMetadata(input_file)
    md = process_sbusndimpacts.get_metadata(ds_short_name= 'sbusndimpacts')
    expected_metadata = {'ShortName': 'sbusndimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'netCDF-3',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        # start_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[0]
        start_date = self.process_sbusndimpacts.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2020-01-19T00:41:58Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        # stop_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[1]
        stop_date = self.process_sbusndimpacts.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-01-19T01:56:47Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        # file_size = round(self.process_goesrpltavirisng.get_file_size_megabytes(), 2)
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.36)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_gpm = self.process_sbusndimpacts
        # wnes = process_geos.get_wnes_geometry()
        wnes = process_gpm.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '41.529')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-73.03')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '40.965')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-70.869')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'd18eaf900b9fc955d1839daddc492e7f')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltavirisng
        :return: metadata object 
        """

        metadata = self.process_sbusndimpacts.get_metadata(ds_short_name='sbusndimpacts',
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

