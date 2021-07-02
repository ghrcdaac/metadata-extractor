from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_goesimpacts import ExtractGoesimpactsMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for this file:
#host=thor,env=ops,project=IMPACTS,ds=goesimpacts,inv=inventory,file=IMPACTS_goes16_conus_20200102_002856_ch08.nc,path=CONUS/ch08/20200102/IMPACTS_goes16_conus_20200102_002856_ch08.nc,size=1702659,start=2020-01-02T00:26:18Z,end=2020-01-02T00:28:56Z,browse=N,checksum=4f7a32c31c618da8ba1ce7e7c233092acb7a7aa5,NLat=49.97999954223633,SLat=25.0,WLon=-105.0,ELon=-65.02000427246094,format=netCDF-4

class TestProcessGoesimpacts(TestCase):
    """
    Test processing goesimpacts.
    This will test if goesimpacts metadata will be extracted correctly
    """
    granule_name = "IMPACTS_goes16_conus_20200102_002856_ch08.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_goesimpacts = ExtractGoesimpactsMetadata(input_file)
    md = process_goesimpacts.get_metadata(ds_short_name= 'goesimpacts')
    expected_metadata = {'ShortName': 'goesimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'netCDF-4',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        # start_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[0]
        start_date = self.process_goesimpacts.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2020-01-02T00:26:18Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        # stop_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[1]
        stop_date = self.process_goesimpacts.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-01-02T00:28:56Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        # file_size = round(self.process_goesrpltavirisng.get_file_size_megabytes(), 2)
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 1.70)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_goes = self.process_goesimpacts
        # wnes = process_geos.get_wnes_geometry()
        wnes = process_goes.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '49.98')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-105.0')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '25.0')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-65.02')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '064f0d1a9569dbd6db06b1e5ac2f0b60')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltavirisng
        :return: metadata object 
        """

        metadata = self.process_goesimpacts.get_metadata(ds_short_name='goesimpacts',
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
