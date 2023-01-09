from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_imergcpex import ExtractImergcpexMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for sample file:
#host=thor,env=ops,project=CPEX,ds=imergcpex,inv=inventory,file=CPEX_3B_HHR_E_MS_MRG_3IMERG_20170715_S150000_E152959_0900_V04B.nc,path=20170715/CPEX_3B_HHR_E_MS_MRG_3IMERG_20170715_S150000_E152959_0900_V04B.nc,size=773884,start=2017-07-15T15:00:00Z,end=2017-07-15T15:29:59Z,browse=N,checksum=15e825fba120d3b47df89bcee952f16671eee93e,NLat=39.95000076293945,SLat=5.050000190734863,WLon=-99.94999694824219,ELon=-45.04999923706055,format=netCDF-3
class TestProcessImergcpex(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "CPEX_3B_HHR_E_MS_MRG_3IMERG_20170715_S150000_E152959_0900_V04B.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractImergcpexMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'imergcpex')
    expected_metadata = {'ShortName': 'imergcpex',
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
        self.assertEqual(start_date, "2017-07-15T15:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2017-07-15T15:29:59Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 0.77)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))
    
    #NLat=39.95000076293945,SLat=5.050000190734863,WLon=-99.94999694824219,ELon=-45.04999923706055
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '39.95')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-99.95')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '5.05')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-45.05')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, 'a7ca1ab49207d648c74bb0c3cb547252')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='imergcpex',
                                                     format='netCDF-3', version='1')
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
