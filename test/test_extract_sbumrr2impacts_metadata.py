from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_sbumrr2impacts import ExtractSbumrr2impactsMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML

#host=thor,env=ops,project=IMPACTS,ds=sbumrr2impacts,inv=inventory,file=IMPACTS_SBU_mrr2_20200106_BNL.nc,path=MRR2_BNL/IMPACTS_SBU_mrr2_20200106_BNL.nc,size=95724732,start=2020-01-06T00:01:00Z,end=2020-01-07T00:00:00Z,browse=N,checksum=1bc64c52844639f44a31206238c057d1ed90b2df,NLat=40.888999999999996,SLat=40.869,WLon=-72.884,ELon=-72.86399999999999,format=netCDF-3
# this file IMPACTS_SBU_mrr2_20200106_BNL.nc size is too large to check in
# to git, so I created a fake to this file, only take time field in the 
# original file. As a result, checksum and file size will change
class TestProcessSbumrr2impacts(TestCase):
    """
    Test processing sbumrr2impacts.
    This will test if sbumrr2impacts metadata will be extracted correctly
    """
    #granule_name = "IMPACTS_SBU_mrr2_20200106_BNL.nc"
    granule_name = "fake_IMPACTS_SBU_mrr2_20200106_BNL.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_sbumrr2impacts = ExtractSbumrr2impactsMetadata(input_file)
    md = process_sbumrr2impacts.get_metadata(ds_short_name= 'sbumrr2impacts')
    expected_metadata = {'ShortName': 'sbumrr2impacts',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'netCDF-3',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        # start_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[0]
        start_date = self.process_sbumrr2impacts.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        #self.assertEqual(start_date, "2020-01-04T00:00:01Z")
        self.assertEqual(start_date, "2020-01-06T00:01:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        # stop_date = self.process_goesrpltavirisng.get_temporal(units_variable=self.time_units)[1]
        stop_date = self.process_sbumrr2impacts.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        #self.assertEqual(stop_date, "2020-01-04T23:58:17Z")
        self.assertEqual(stop_date, "2020-01-07T00:00:00Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        # file_size = round(self.process_goesrpltavirisng.get_file_size_megabytes(), 2)
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        #self.assertEqual(file_size, 95.72)
        self.assertEqual(file_size, 0.01)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_sbumrr2 = self.process_sbumrr2impacts
        # wnes = process_geos.get_wnes_geometry()
        wnes = process_sbumrr2.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '40.889')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-72.884')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '40.869')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-72.864')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_goesrpltavirisng.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        #self.assertEqual(checksum, '2a17ed1639d46a900cc0235c45828b95')
        #this is for fake file
        self.assertEqual(checksum, '2393431f8671a6409d3739f80d3bcd9c')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of goesrpltavirisng
        :return: metadata object 
        """

        metadata = self.process_sbumrr2impacts.get_metadata(ds_short_name='sbumrr2impacts',
                                                     format='netCDF-3', version='1')
        for key in self.expected_metadata.keys():
            self.assertEqual(metadata[key], self.expected_metadata[key])

    def test_a1_generate_echo10(self):
        """
        Test generate the echo 10 in tmp folder
        """
        self.expected_metadata['OnlineAccessURL'] = "http://localhost.com"
        echo10xml = GenerateEcho10XML(self.expected_metadata)
        echo10xml.generate_echo10_xml_file()
        self.assertTrue(path.exists(f'/tmp/{self.granule_name}.cmr.xml'))

