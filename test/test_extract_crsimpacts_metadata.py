from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_crsimpacts import ExtractCrsimpactsMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for sample file:
#host=thor,env=ops,project=IMPACTS,ds=crsimpacts,inv=inventory,file=IMPACTS_CRS_L1B_RevA_20200207T122227_to_20200207T180109.h5,path=IMPACTS_CRS_L1B_RevA_20200207T122227_to_20200207T180109.h5,size=1273998851,start=2020-02-07T12:22:27Z,end=2020-02-07T18:01:09Z,browse=N,checksum=aa45afc47198af3c2507931fd8dd961fe4bee024,NLat=43.15413284301758,SLat=33.06058883666992,WLon=-79.9180679321289,ELon=-73.18217468261719,format=HDF-5

class TestProcessCrsimpacts(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    #granule_name = "IMPACTS_CRS_L1B_RevA_20200207T122227_to_20200207T180109.h5"
    granule_name = "IMPACTS_CRS_L1B_RevA_20200207T122227_to_20200207T180109_subset.h5"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractCrsimpactsMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'crsimpacts')
    expected_metadata = {'ShortName': 'crsimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'HDF-5',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2020-02-07T12:22:27Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-02-07T18:01:09Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        #self.assertEqual(file_size, 1274.00)
        self.assertEqual(file_size, 1.31)

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
        self.assertEqual(north, '43.154')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-79.918')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '33.061')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-73.182')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        #self.assertEqual(checksum, '160097b5c20660cb0e0bebfceda5936a')
        self.assertEqual(checksum, '71af631d70391f3f67a52dc87b97a93e')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='crsimpacts',
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
