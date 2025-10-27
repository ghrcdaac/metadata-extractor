from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_cplimpacts import ExtractCplimpactsMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#host=thor,env=ops,project=IMPACTS,ds=cplimpacts,inv=inventory,file=IMPACTS_CPL_L2_V1-02_01kmLay_20200115.hdf5,path=IMPACTS_CPL_L2_V1-02_01kmLay_20200115.hdf5,size=7944380,start=2020-01-15T17:55:40Z,end=2020-01-15T21:33:13Z,browse=N,checksum=e2b20659fd6334551e3b60d61473dee2d9cdba69,NLat=35.06972885131836,SLat=33.060611724853516,WLon=-117.23030853271484,ELon=-85.32685852050781,format=HDF-5

#host=thor,env=ops,project=IMPACTS,ds=cplimpacts,inv=inventory,file=IMPACTS_CPL_L2_V1-02_01kmPro_20200223.hdf5,path=IMPACTS_CPL_L2_V1-02_01kmPro_20200223.hdf5,size=95874768,start=2020-02-23T16:55:15Z,end=2020-02-23T18:05:45Z,browse=N,checksum=3b1a5e8efff43c683ee6f562c0af654f7566b762,NLat=34.57469177246094,SLat=32.857879638671875,WLon=-79.60249328613281,ELon=-76.13682556152344,format=HDF-5

#host=thor,env=ops,project=IMPACTS,ds=cplimpacts,inv=inventory,file=IMPACTS_CPL_ATB_L1_20200223.hdf5,path=IMPACTS_CPL_ATB_L1_20200223.hdf5,size=395455404,start=2020-02-23T16:55:15Z,end=2020-02-23T18:05:45Z,browse=N,checksum=93e5c601f4c0d6b4b74876be53ce21254cd3dc6c,NLat=34.574790954589844,SLat=32.857879638671875,WLon=-79.60249328613281,ELon=-76.13679504394531,format=HDF-5

# Three types of files are tested and passed. 
# Only IMPACTS_CPL_L2_V1-02_01kmLay_20200115.hdf5 is checked in git repo
# The other two files (ATB and 01kmPro) are deleted from fixture directory
class TestProcessCplimpacts(TestCase):
    """
    Test processing cplimpacts.
    This will test if cplimpacts metadata will be extracted correctly
    """
    granule_name = "IMPACTS_CPL_L2_V1-02_01kmLay_20200115.hdf5"
    #granule_name = "IMPACTS_CPL_L2_V1-02_01kmPro_20200223.hdf5"
    #granule_name = "IMPACTS_CPL_ATB_L1_20200223.hdf5"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_cplimpacts = ExtractCplimpactsMetadata(input_file)
    md = process_cplimpacts.get_metadata(ds_short_name= 'cplimpacts')
    expected_metadata = {'ShortName': 'cplimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'HDF-5',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        # start_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[0]
        start_date = self.process_cplimpacts.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2020-01-15T17:55:40Z")
        #self.assertEqual(start_date, "2020-02-23T16:55:15Z")
        #self.assertEqual(start_date, "2020-02-23T16:55:15Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        # stop_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[1]
        stop_date = self.process_cplimpacts.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-01-15T21:33:13Z")
        #self.assertEqual(stop_date, "2020-02-23T18:05:45Z")
        #self.assertEqual(stop_date, "2020-02-23T18:05:45Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        # file_size = round(self.process_goesrpltavirisng.get_file_size_megabytes(), 2)
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 7.94)
        #self.assertEqual(file_size, 95.87)
        #self.assertEqual(file_size, 395.46)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_cpl = self.process_cplimpacts
        # wnes = process_geos.get_wnes_geometry()
        wnes = process_cpl.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '35.07')
        #self.assertEqual(north, '34.575')
        #self.assertEqual(north, '34.575')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-117.23')
        #self.assertEqual(west, '-79.602')
        #self.assertEqual(west, '-79.602')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '33.061')
        #self.assertEqual(south, '32.858')
        #self.assertEqual(south, '32.858')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-85.327')
        #self.assertEqual(east, '-76.137')
        #self.assertEqual(east, '-76.137')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'd685334723345765489502716dab9d55')
        #self.assertEqual(checksum, '2aaed99097471eb9dea5950659b7a4cb')
        #self.assertEqual(checksum, 'be43ff8c8630e7f204d32a40977dd2a4')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltavirisng
        :return: metadata object 
        """

        metadata = self.process_cplimpacts.get_metadata(ds_short_name='cplimpacts',
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
