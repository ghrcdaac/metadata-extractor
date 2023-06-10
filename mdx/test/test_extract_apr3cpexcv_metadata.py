from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_apr3cpexcv import ExtractApr3cpexcvMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for sample file:
#host=thor,env=ops,project=CPEX-CV,ds=apr3cpexcv,inv=inventory,file=cpexcv_APR3_DC8_20220915_R0_S20220915a161031_E20220915a161201_KUsKAs.nc,path=20220915/cpexcv_APR3_DC8_20220915_R0_S20220915a161031_E20220915a161201_KUsKAs.nc,size=17453380,start=2022-09-15T16:10:31Z,end=2022-09-15T16:12:01Z,browse=Y,checksum=8aaa7c79a196197cbb7d97bf74f40b8cbd281eb2,NLat=19.638779161040343,SLat=19.455066909256253,WLon=-19.886060475141978,ELon=-19.66113084017884,format=netCDF-4
class TestProcessApr3cpexcv(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "cpexcv_APR3_DC8_20220915_R0_S20220915a161031_E20220915a161201_KUsKAs.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractApr3cpexcvMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'apr3cpexcv')
    expected_metadata = {'ShortName': 'apr3cpexcv',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'netCDF-4',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2022-09-15T16:10:31Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2022-09-15T16:12:01Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 8.97)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=19.638779161040343,SLat=19.455066909256253,WLon=-19.886060475141978,ELon=-19.66113084017884
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '19.639')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-19.886')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '19.455')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-19.661')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'e7dad6eb3748f4a989db4270fc2ab674')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='apr3cpexcv',
                                                     format='netCDF-4', version='1')
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
