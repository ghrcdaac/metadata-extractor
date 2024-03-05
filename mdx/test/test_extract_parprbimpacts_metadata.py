from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_parprbimpacts import ExtractParprbimpactsMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

##prem metadata for sample file:
##host=thor,env=ops,project=IMPACTS,ds=parprbimpacts,inv=inventory,file=IMPACTS_2DSV-P3_20200218_sizedistributions_v01.nc,path=IMPACTS_2DSV-P3_20200218_sizedistributions_v01.nc,size=7159723,start=2020-02-18T17:22:00Z,end=2020-02-18T22:13:00Z,browse=Y,checksum=4f1b7600fca30009f2d8c78884d2fddbfe6f5b66,NLat=45.249881744384766,SLat=37.910213470458984,WLon=-75.5810317993164,ELon=-70.19293975830078,format=netCDF-4

class TestProcessParprbimpacts(TestCase):
    """
    Test processing dataset metadata.
    This will test if metadata will be extracted correctly
    """
    granule_name = "IMPACTS_2DSV-P3_20200218_sizedistributions_v01.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractParprbimpactsMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'parprbimpacts')
    expected_metadata = {'ShortName': 'parprbimpacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'netCDF-4',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal(self.granule_name)[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2020-02-18T17:22:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        stop_date = self.process_dataset.get_temporal(self.granule_name)[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2020-02-18T22:13:00Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 7.16)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_gpm = self.process_dataset
        wnes = process_gpm.get_wnes_geometry(self.granule_name)
        print('wnes ',wnes)
        return str(round(float(wnes[index]), 3))

    #NLat=45.249881744384766,SLat=37.910213470458984,WLon=-75.5810317993164,ELon=-70.19293975830078
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '45.25')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-75.581')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '37.91')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-70.193')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '04af0020aca73a422d1fc95274a25e0f')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltavirisng
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='parprbimpacts',
                                                     format='netCDF-4', version='1')
        for key in self.expected_metadata.keys():
            self.assertEqual(metadata[key], self.expected_metadata[key])

    def test_a1_generate_umm_json(self):
        """
        Test generate the umm json in tmp folder
        """
        self.expected_metadata['OnlineAccessURL'] = "http://localhost.com"
        umm_json = GenerateUmmGJson(self.expected_metadata)
        print(umm_json)
        umm_json.generate_umm_json_file()
        self.assertTrue(path.exists(f'/tmp/{self.granule_name}.cmr.json'))
