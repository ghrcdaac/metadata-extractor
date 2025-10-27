from os import path
from unittest import TestCase
from mdx.granule_metadata_extractor.processing.process_gpmpal import ExtractGpmpalMetadata
from mdx.granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

#prem metadata for sample file:
#host=thor,env=ops,project=NOT APPLICABLE,ds=gpmpal,inv=inventory,file=PAL_precip_wind_TPOS_Argo_float_19644_v1.nc,path=PAL_precip_wind_TPOS_Argo_float_19644_v1.nc,size=19707348,start=2020-09-15T12:00:00Z,end=2021-05-01T12:00:00Z,browse=N,checksum=4f0bd0b03702873ff3baf61843d51042ffc253d7,NLat=2.469,SLat=1.377,WLon=-179.901,ELon=-157.821,format=netCDF-4
class TestProcessGpmpal(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    granule_name = "PAL_precip_wind_TPOS_Argo_float_19644_v1.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_dataset = ExtractGpmpalMetadata(input_file)
    md = process_dataset.get_metadata(ds_short_name= 'gpmpal')
    expected_metadata = {'ShortName': 'gpmpal',
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

        self.assertEqual(start_date, "2020-09-15T12:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2021-05-01T12:00:00Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 2.64)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_sbupl = self.process_dataset
        wnes = process_sbupl.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=2.469,SLat=1.377,WLon=-179.901,ELon=-157.821
    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '2.469')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-179.901')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '1.377')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-157.821')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '64f70bdbf9fb28219259ef4688f811a0')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of sbuplimpacts
        :return: metadata object 
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='gpmpal',
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
