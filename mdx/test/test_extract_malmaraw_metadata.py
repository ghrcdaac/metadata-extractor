from os import path
from unittest import TestCase

from granule_metadata_extractor.processing.process_malmaraw import ExtractMalmaRawMetadata
from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson

class TestProcessMalmaRaw(TestCase):
    """
    Tests for MDX processing of MALMA raw granules
    """
    granule_name = "LH_WFF_UMD_UAS_210103_103000.dat.gz"
    input_file = path.join(path.dirname(__file__), f"fixtures/{granule_name}")
    time_var_key = "time"
    lon_var_key = "lon"
    lat_var_key = "lat"
    time_units = "units"
    date_format = "%Y-%m-%dT%H:%M:%SZ"
    process_malmaraw = ExtractMalmaRawMetadata(input_file)
    expected_metadata = {
        "ShortName": "malmaraw",
        "GranuleUR": granule_name,
        "VersionId": "1", 
        "DataFormat": "Binary"
    }


    def test_1_get_start_date(self):
        start_date = self.process_malmaraw.get_temporal()[0]
        self.expected_metadata["BeginningDateTime"] = start_date
        self.assertEqual(start_date, "2021-01-03T10:30:00Z")

    
    def test_2_get_end_date(self):
        end_date = self.process_malmaraw.get_temporal()[1]
        self.expected_metadata["EndingDateTime"] = end_date
        self.assertEqual(end_date, "2021-01-03T10:39:59Z")


    def test_3_get_file_size(self):
        file_size = round(self.process_malmaraw.get_file_size_megabytes(), 2)
        self.expected_metadata["SizeMBDataGranule"] = str(file_size)
        self.assertEqual(file_size, 6.19)


    def get_wnes(self, index):
            wnes = self.process_malmaraw.get_wnes_geometry()
            return str(round(wnes[index], 3))


    def test_4_get_west(self):
        west = self.get_wnes(0)
        self.expected_metadata["WestBoundingCoordinate"] = west
        self.assertEqual(west, "-76.557")
        
    
    def test_5_get_north(self):
        north = self.get_wnes(1)
        self.expected_metadata['NorthBoundingCoordinate'] = north
        self.assertEqual(north, "38.316")


    def test_6_get_east(self):
        east = self.get_wnes(2)
        self.expected_metadata['EastBoundingCoordinate'] = east
        self.assertEqual(east, "-76.555")


    def test_7_get_south(self):
        south = self.get_wnes(3)
        self.expected_metadata['SouthBoundingCoordinate'] = south
        self.assertEqual(south, "38.314")


    def test_5_get_checksum(self):
        checksum = self.process_malmaraw.get_checksum()
        self.expected_metadata["checksum"] = checksum
        self.assertEqual(checksum, "973c6b41d8817312d8f1b12f76adb314")


    def test_9_generate_metadata(self):
        metadata = self.process_malmaraw.get_metadata(ds_short_name="malmaraw", format="Binary", version="1")
        for key in self.expected_metadata.keys():
            self.assertEqual(metadata[key], self.expected_metadata[key])


    def test_a1_generate_umm_json(self):
        self.expected_metadata["OnlineAccessURL"] = "http://localhost.com"
        umm_json = GenerateUmmGJson(self.expected_metadata)
        umm_json.generate_umm_json_file()
        self.assertTrue(path.exists(f"/tmp/{self.granule_name}.cmr.json"))
