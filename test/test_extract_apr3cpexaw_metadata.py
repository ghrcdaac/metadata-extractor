from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_apr3cpexaw import ExtractApr3cpexawMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for sample file:
#host=thor,env=ops,project=CPEX-AW,ds=apr3cpexaw,inv=inventory,file=CPEXAW_APR3_S20210820a192822u_20210820a205432uw_c1.mat,path=2021-08-20/CPEXAW_APR3_S20210820a192822u_20210820a205432uw_c1.mat,size=28252982,start=2021-08-20T19:28:23Z,end=2021-08-20T20:54:32Z,browse=Y,checksum=74591420592eaa2a8c30608cb9f4f1a924bc7142,NLat=22.782537083725686,SLat=16.471595764,WLon=-67.66011670709443,ELon=-65.503921509,format=MAT

#host=thor,env=ops,project=CPEX-AW,ds=apr3cpexaw,inv=inventory,file=CPEXAW_APR3_S20210820a192822u_20210820a205432uw_c1_reduced.mat,path=CPEXAW_APR3_S20210820a192822u_20210820a205432uw_c1_reduced.mat,size=55280,start=2021-08-20T19:28:23Z,end=2021-08-20T20:54:32Z,browse=Y,checksum=dabce6fba3ede9802ed3977ceb926c66cbe64a6a,NLat=22.782537083725686,SLat=16.471595764,WLon=-67.66011670709443,ELon=-65.503921509,format=MAT
class TestProcessApr3cpexaw(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "CPEXAW_APR3_S20210820a192822u_20210820a205432uw_c1_reduced.mat"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractApr3cpexawMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'apr3cpexaw')
    expected_metadata = {'ShortName': 'apr3cpexaw',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'MAT',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2021-08-20T19:28:23Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2021-08-20T20:54:32Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.06)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=22.782537083725686,SLat=16.471595764,WLon=-67.66011670709443,ELon=-65.503921509
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '22.783')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-67.66')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '16.472')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-65.504')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'b4624f72bae3ebd536576a7f4aaafb73')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='apr3cpexaw',
                                                     format='MAT', version='1')
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
