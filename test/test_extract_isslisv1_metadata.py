from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_isslisv1 import ExtractIsslisv1Metadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML

#host=thor,env=ops,project=IMPACTS,ds=isslis,inv=inventory,file=ISS_LIS_SC_V1.0_20201015_000413_NRT_21494.nc,path=ISS_LIS_SC_V1.0_20201015_000413_NRT_21494.nc,size=416816,start=2020-10-15T00:05:55Z,end=2020-10-15T00:06:17Z,browse=N,checksum=70d607446319b449fa668adab1e49e61f50fbd88,NLat=12.25,SLat=2.25,WLon=90.75,ELon=100.75,format=netCDF-4

#host=thor,env=ops,project=IMPACTS,ds=isslis,inv=inventory,file=ISS_LIS_SC_V1.0_20201015_000413_NRT_21494.hdf,path=ISS_LIS_SC_V1.0_20201015_000413_NRT_21494.hdf,size=58146,start=2020-10-15T00:05:55Z,end=2020-10-15T00:06:17Z,browse=N,checksum=450ff414f3b8e5c69149a49b9458cad06fbdd7f5,NLat=12.25,SLat=2.25,WLon=90.75,ELon=100.75,format=netCDF-4

#host=thor,env=ops,project=IMPACTS,ds=isslis,inv=inventory,file=ISS_LIS_SC_V1.0_20201015_NQC_21495.nc,path=ISS_LIS_SC_V1.0_20201015_NQC_21495.nc,size=1729377,start=2020-10-15T01:13:12Z,end=2020-10-15T02:46:04Z,browse=N,checksum=dfbd6f6cc5ad90bd2505d71e21b4946fd08a2ebe,NLat=55.25,SLat=-55.25,WLon=-179.75,ELon=179.75,format=netCDF-4

#host=thor,env=ops,project=IMPACTS,ds=isslis,inv=inventory,file=ISS_LIS_SC_V1.0_20201015_NQC_21495.hdf,path=ISS_LIS_SC_V1.0_20201015_NQC_21495.hdf,size=1173673,start=2020-10-15T01:13:12Z,end=2020-10-15T02:46:04Z,browse=N,checksum=60425c1cb44c7a57c8f27def243fc513b8ea965f,NLat=55.25,SLat=-55.25,WLon=-179.75,ELon=179.75,format=netCDF-4

# Three types of files are tested and passed. 
# Only IMPACTS_CPL_L2_V1-02_01kmLay_20200115.hdf5 is checked in git repo
# The other two files (ATB and 01kmPro) are deleted from fixture directory
class TestProcessIsslisv1(TestCase):
    """
    Test processing isslisv1.
    This will test if isslisv1 metadata will be extracted correctly
    """
    #granule_name = "ISS_LIS_SC_V1.0_20201015_000413_NRT_21494.nc"
    #granule_name = "ISS_LIS_SC_V1.0_20201015_000413_NRT_21494.hdf"
    #granule_name = "ISS_LIS_SC_V1.0_20201015_NQC_21495.nc"
    granule_name = "ISS_LIS_SC_V1.0_20201015_NQC_21495.hdf"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_isslisv1 = ExtractIsslisv1Metadata(input_file)
    # test for NRT NC file
    #md = process_isslisv1.get_metadata(ds_short_name= 'isslis_v1_nrt')
    #expected_metadata = {'ShortName': 'isslis_v1_nrt',
    #                     'GranuleUR': granule_name,
    #                     'VersionId': '1', 'DataFormat': 'netCDF-4',
    #                     }
    # test for NRT HDF4 file
    #md = process_isslisv1.get_metadata(ds_short_name= 'isslis_v1_nrt')
    #expected_metadata = {'ShortName': 'isslis_v1_nrt',
    #                     'GranuleUR': granule_name,
    #                     'VersionId': '1', 'DataFormat': 'HDF-4',
    #                     }
    # test for NQC nc file
    #md = process_isslisv1.get_metadata(ds_short_name= 'isslis_v1_nqc')
    #expected_metadata = {'ShortName': 'isslis_v1_nqc',
    #                     'GranuleUR': granule_name,
    #                     'VersionId': '1', 'DataFormat': 'netCDF-4',
    #                     }
    # test for NQC hdf file
    md = process_isslisv1.get_metadata(ds_short_name= 'isslis_v1_nqc')
    expected_metadata = {'ShortName': 'isslis_v1_nqc',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'HDF-4',
                         }



    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        # start_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[0]
        start_date = self.process_isslisv1.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        #self.assertEqual(start_date, "2020-10-15T00:05:55Z")
        #self.assertEqual(start_date, "2020-10-15T00:05:55Z")
        #self.assertEqual(start_date, "2020-10-15T01:13:12Z")
        self.assertEqual(start_date, "2020-10-15T01:13:12Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        # stop_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[1]
        stop_date = self.process_isslisv1.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        #self.assertEqual(stop_date, "2020-10-15T00:06:17Z")
        #self.assertEqual(stop_date, "2020-10-15T00:06:17Z")
        #self.assertEqual(stop_date, "2020-10-15T02:46:04Z")
        self.assertEqual(stop_date, "2020-10-15T02:46:04Z")
    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        # file_size = round(self.process_goesrpltavirisng.get_file_size_megabytes(), 2)
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        #self.assertEqual(file_size, 0.42)
        #self.assertEqual(file_size, 0.06)
        #self.assertEqual(file_size, 1.73)
        self.assertEqual(file_size, 1.17)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_isslis = self.process_isslisv1
        # wnes = process_geos.get_wnes_geometry()
        wnes = process_isslis.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        #self.assertEqual(north, '12.25')
        #self.assertEqual(north, '12.25')
        #self.assertEqual(north, '55.25')
        self.assertEqual(north, '55.25')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        #self.assertEqual(west, '90.75')
        #self.assertEqual(west, '90.75')
        #self.assertEqual(west, '-179.75')
        self.assertEqual(west, '-179.75')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        #self.assertEqual(south, '2.25')
        #self.assertEqual(south, '2.25')
        #self.assertEqual(south, '-55.25')
        self.assertEqual(south, '-55.25')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        #self.assertEqual(east, '100.75')
        #self.assertEqual(east, '100.75')
        #self.assertEqual(east, '179.75')
        self.assertEqual(east, '179.75')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        #self.assertEqual(checksum, '4a0c935c6f3ed43a4673d2a4e9cbf797')
        #self.assertEqual(checksum, 'b013e2b87bc9ccdf88781f5596e07741')
        #self.assertEqual(checksum, 'eea2976598ce8f91207ad2ab434ac48a')
        self.assertEqual(checksum, 'd4fc04cd975dc67a122f8d5f143f4f71')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltavirisng
        :return: metadata object 
        """

        #metadata = self.process_isslisv1.get_metadata(ds_short_name='isslis_v1_nrt',
        #                                             format='netCDF-4', version='1')
        metadata = self.process_isslisv1.get_metadata(ds_short_name='isslis_v1_nqc',
                                                     format='netCDF-4', version='1')

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
