from os import path
from unittest import TestCase
from granule_metadata_extractor.processing.process_tcspecmwf import ExtractTcspecmwfMetadata
from granule_metadata_extractor.src.generate_echo10_xml import GenerateEcho10XML


class TestProcessTcspecmwf(TestCase):
    """
    Test processing Geosr.
    This will test if tcspecmwf metadata will be extracted correctly
    """
    granule_name = "tcspecmwf_2005_180_daily.tar"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    process_tcspecmwf = ExtractTcspecmwfMetadata(input_file)
    md = process_tcspecmwf.get_metadata(ds_short_name= 'tcspecmwf')
    expected_metadata = {'ShortName': 'tcspecmwf',
                         'GranuleUR': granule_name,
                         'VersionId': '1', 'DataFormat': 'GRIB',
                         }

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        # start_date = self.process_tcspecmwf.get_temporal(units_variable=self.time_units)[0]
        start_date = self.process_tcspecmwf.get_temporal_lookup(self.granule_name)[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2005-06-29T00:00:00Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct start date
        :return:
        """
        # stop_date = self.process_tcspecmwf.get_temporal(units_variable=self.time_units)[1]
        stop_date = self.process_tcspecmwf.get_temporal_lookup(self.granule_name)[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2005-06-29T21:00:00Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        # file_size = round(self.process_tcspecmwf.get_file_size_megabytes(), 2)
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 1550.93)

    def get_wnes(self, index):
        """
        A function helper to get North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_tcspecmwf
        # wnes = process_geos.get_wnes_geometry()
        wnes = process_geos.get_wnes_geometry_lookup(self.granule_name)
        return str(round(float(wnes[index]), 3))

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '90.0')

    def test_5_get_west(self):
        """29.864
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '-180.0')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '-90.0')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '180.0')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        # checksum = self.process_tcspecmwf.get_checksum()
        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '787a322ddafcb5e565af144d4c4aee0b')

    def test_9_generate_metadata(self):
        """
        Test generating metadata of aces1am
        :return: metadata object 
        """

        metadata = self.process_tcspecmwf.get_metadata(ds_short_name='tcspecmwf',
                                                     format='GRIB', version='1')
        for key in self.expected_metadata.keys():
            self.assertEqual(metadata[key], self.expected_metadata[key])

    def test_a1_generate_echo10(self):
        """
        Test generate the echo 10 in tmp folder
        :return: None
        """
        self.expected_metadata['OnlineAccessURL'] = "http://localhost.com"
        echo10xml = GenerateEcho10XML(self.expected_metadata)
        echo10xml.generate_echo10_xml_file()
        self.assertTrue(path.exists(f'/tmp/{self.granule_name}.cmr.xml'))

if __name__ == '__main__':
    test = TestProcessTcspecmwf()
    test.test_3_get_file_size()
