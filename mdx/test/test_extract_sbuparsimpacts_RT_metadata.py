from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_sbuparsimpacts import ExtractSbuparsimpactsMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for sample file:
#host=thor,env=ops,project=IMPACTS,ds=sbuparsimpacts,inv=inventory,file=IMPACTS_SBU_parsivel_20200112_RT.nc,path=Parsivel_RadarTruck/IMPACTS_SBU_parsivel_20200112_RT.nc,size=2633108,start=2020-01-12T00:00:00Z,end=2020-01-12T10:30:00Z,browse=N,checksum=9116c43c2157b881b842cd2c5dd028b45870d8c9,NLat=40.8973388671875,SLat=40.896730518751404,WLon=-73.12752532958984,ELon=-73.1267305187514,format=netCDF-3

class TestProcessSbuparsimpactsNetCDF(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "IMPACTS_SBU_parsivel_20200112_RT.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractSbuparsimpactsMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'sbuparsimpacts')
    expected_metadata = {'ShortName': 'sbuparsimpacts',
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

        self.assertEqual(start_date, "2020-01-12T00:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-01-12T10:30:00Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 2.63)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=40.8973388671875,SLat=40.896730518751404,WLon=-73.12752532958984,ELon=-73.1267305187514

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '40.897')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-73.128')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '40.897')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-73.127')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '02946f5010ba756e9a94bf39ce826666')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltavirisng
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='sbuparsimpacts',
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
