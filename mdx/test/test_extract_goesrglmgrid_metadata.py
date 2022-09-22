from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_goesrglmgrid import ExtractGoesrglmgridMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML

#host=thor,env=ops,project=NOT APPLICABLE,ds=goesrglmgrid,inv=inventory,file=OR_GLM-L3-GLMF-M6_G16_T05_e20210807162700.nc,path=OR_GLM-L3-GLMF-M6_G16_T05_e20210807162700.nc,size=59428,start=2021-08-07T16:22:00Z,end=2021-08-07T16:27:00Z,browse=N,checksum=1eb38796a43ffa5a6d10ee25e6591cfd1afcda4b,NLat=57.31810872493771,SLat=36.68194510016496,WLon=-150.50109043769376,ELon=-96.8534546275037,format=netCDF-4

class TestProcessGoesrglmgrid(TestCase):
    """
    Test processing goesrglmgrid for nc file
    This will test if goesrglmgrid metadata will be extracted correctly
    """
    granule_name = "OR_GLM-L3-GLMF-M6_G16_T05_e20210807162700.nc"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_instance = ExtractGoesrglmgridMetadata(input_file)

    md = process_instance.get_metadata(ds_short_name= 'goesrglmgrid')
    expected_metadata = {'ShortName': 'goesrglmgrid',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'netCDF-4',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_instance.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2021-08-07T16:22:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_instance.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date
      
        self.assertEqual(stop_date, "2021-08-07T16:27:00Z")

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
        wnes = self.process_instance.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=57.31810872493771,SLat=36.68194510016496,WLon=-150.50109043769376,ELon=-96.8534546275037

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '57.318')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-150.501')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '36.682')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '-96.853')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '5fbf552c0d0c1164e038252557ebdc83')

    def test_9_generate_metadata(self):
        """
        :return: metadata object 
        """
        metadata = self.process_instance.get_metadata(ds_short_name='goesrglmgrid',
                                                     format='netCDF-4', version='1')

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

