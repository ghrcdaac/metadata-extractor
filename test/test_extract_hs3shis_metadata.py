from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_hs3shis import ExtractHs3shisMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML

#prem metadata for sample file:
#host=thor,env=ops,project=HS3,ds=hs3shis,inv=inventory,file=SHIS_rdr20110910T012413end20110910T021638sdr20160427T062645_rad.nc,path=2011/SHIS_rdr20110910T012413end20110910T021638sdr20160427T062645_rad.nc,size=583380,start=2011-09-10T01:24:13Z,end=2011-09-10T02:16:38Z,browse=N,checksum=0c7fe7efd5bb85443d091b65874c20e9520e8429,NLat=34.775203704833984,SLat=34.53623962402344,WLon=-118.97808074951172,ELon=-118.94039154052734,format=netCDF-3

class TestProcessHs3shis(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "SHIS_rdr20110910T012413end20110910T021638sdr20160427T062645_rad.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractHs3shisMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'hs3shis')
    expected_metadata = {'ShortName': 'hs3shis',
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

        self.assertEqual(start_date, "2011-09-10T01:24:13Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2011-09-10T02:16:38Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.58)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=34.775203704833984,SLat=34.53623962402344,WLon=-118.97808074951172,ELon=-118.94039154052734

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '34.775')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-118.978')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '34.536')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-118.94')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '9b8b4767e4b4fc7b1d1230c0e2679cbd')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='hs3shis',
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
