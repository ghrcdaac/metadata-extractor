from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_prsondecpexaw import ExtractPrsondecpexawMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for sample file:
#host=thor,env=ops,project=CPEX-AW,ds=prsondecpexaw,inv=inventory,file=PR_Radiosonde_CPEXAW_qc_uprm034_20210928.csv,path=PR_Radiosonde_CPEXAW_qc_uprm034_20210928.csv,size=375216,start=2021-09-28T22:30:14Z,end=2021-09-28T23:46:22Z,browse=Y,checksum=8e050a6587338d56703b58fdcb2d0fb253a6a490,NLat=18.26456,SLat=18.20571,WLon=-67.23106,ELon=-67.07084,format=CSV
class TestProcessPrsondecpexaw(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "PR_Radiosonde_CPEXAW_qc_uprm034_20210928.csv"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractPrsondecpexawMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'prsondecpexaw')
    expected_metadata = {'ShortName': 'prsondecpexaw',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'CSV',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2021-09-28T22:30:14Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2021-09-28T23:46:22Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.38)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))
    
    #NLat=18.26456,SLat=18.20571,WLon=-67.23106,ELon=-67.07084
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '18.265')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-67.231')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '18.206')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-67.071')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '314211032c1ab47c2d5e42a7d3cb0129')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='prsondecpexaw',
                                                     format='CSV', version='1')
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
