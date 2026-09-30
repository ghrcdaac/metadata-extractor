from pathlib import Path
import tempfile
import os
import shutil
import gzip
from unittest import TestCase
from granule_metadata_extractor.processing.process_kwajalein import ExtractKwajaleinMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

FIXTURES_DIR = Path(__file__).parent / "fixtures"
GRANULE_NAME = "KWAJ_2026_0331_235952.cf"
GZ_FILE_PATH = FIXTURES_DIR / f"{GRANULE_NAME}.gz"

class TestProcessKwajalein(TestCase):
    """
    Test processing.
    This will test if metadata will be extracted correctly
    """
    time_var_key = 'time'
    lon_var_key = 'lon'
    lat_var_key = 'lat'
    time_units = 'units'
    date_format = '%Y-%m-%dT%H:%M:%SZ'
    expected_metadata = {'ShortName': 'kwajalein',
                         'GranuleUR': GRANULE_NAME,
                         'VersionId': '1', 'DataFormat': 'netCDF-4',
                         }

    @classmethod
    def setUpClass(cls):
        """Runs once before any test in this class runs."""
        cls.temp_dir = tempfile.mkdtemp()
        cls.decompressed_path= Path(cls.temp_dir) / GRANULE_NAME

        # Setup: Decompress before tests
        with gzip.open(GZ_FILE_PATH, "rb") as f_in:
            with open(cls.decompressed_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        cls.process_dataset = ExtractKwajaleinMetadata(cls.decompressed_path)
        cls.md = cls.process_dataset.get_metadata(ds_short_name= 'kwajalein')

    @classmethod
    def tearDownClass(cls):
        """Runs once after all tests in this class finish."""
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def test_1_get_start_date(self):
        """
        Testing get correct start date
        :return:
        """
        start_date = self.process_dataset.get_temporal()[0]
        self.expected_metadata['BeginningDateTime'] = start_date

        self.assertEqual(start_date, "2026-04-01T00:00:05Z")

    def test_2_get_stop_date(self):
        """
        Testing get correct end date
        :return:
        """
        stop_date = self.process_dataset.get_temporal()[1]
        self.expected_metadata['EndingDateTime'] = stop_date

        self.assertEqual(stop_date, "2026-04-01T00:08:06Z")

    def test_3_get_file_size(self):
        """
        Test geting the correct file size
        :return:
        """
        file_size = float(self.md['SizeMBDataGranule'])
        self.expected_metadata['SizeMBDataGranule'] = str(file_size)
        self.assertEqual(file_size, 9.39)

    def get_wnes(self, index):
        """
        A function helper to get North, West, South, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        process_geos = self.process_dataset
        wnes = process_geos.get_wnes_geometry()
        return str(round(float(wnes[index]), 3))

    #NLat=38.971065521240234,SLat=36.28207778930664,WLon=-80.79434967041016,ELon=-76.9762039184570

    def test_4_get_north(self):
        """
        Test geometry metadata
        :return:
        """
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, '10.599')

    def test_5_get_west(self):
        """
        Test geometry metadata
        :return:
        """
        west = self.get_wnes(0)
        self.expected_metadata['WestBoundingCoordinate'] = west
        self.assertEqual(west, '165.842')

    def test_6_get_south(self):
        """
        Test geometry metadata
        :return:
        """
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, '6.837')

    def test_7_get_east(self):
        """
        Test geometry metadata
        :return:
        """
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, '169.623')

    def test_8_get_checksum(self):
        """
        Test geting the chucksom of the input file
        :return: the MD5 string
        """

        checksum = self.md['checksum']
        self.expected_metadata['checksum'] = checksum
        self.assertEqual(checksum, '0bf25778336833f9472e4148524b91b8')

    def test_9_generate_metadata(self):
        """
        Test generating metadata
        :return: metadata object
        """

        metadata = self.process_dataset.get_metadata(ds_short_name='kwajalein',
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
        self.assertTrue(os.path.exists(f'/tmp/{GRANULE_NAME}.cmr.json'))
