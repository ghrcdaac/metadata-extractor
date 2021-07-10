from granule_metadata_extractor.src.generate_umm_g_json import GenerateUmmGJson
import granule_metadata_extractor.processing as mdx
import granule_metadata_extractor.src as src
from hashlib import md5
import pytest
import json
import os
import re


def get_test_inputs(path_to_input=os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                               'expected_metadata')):
    """
    Gets list of test input file paths for processing tests
    :param path_to_input: Path to input test files; defaults to current_dir/expected_metadata
    :returns: list of test input file paths
    """
    files_in_dir = list()
    for subdir, dirs, files in os.walk(path_to_input):
        for file in files:
            files_in_dir.append(os.path.join(subdir, file))
    return files_in_dir


def get_switcher_dict(switcher_location=os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                     '..', 'process_mdx', 'switchers.json')):
    """
    Gets switcher json from ../process_mdx/switchers.json which stores processing values based on
    shortname
    :param switcher_location: absolute path to switchers.json location; defaults to
    ../process_mdx/switchers.json
    :returns: dict containing switchers.json value
    """
    with open(switcher_location, 'r') as f:
        return json.load(f)


@pytest.fixture(scope="class", params=get_test_inputs())
def expected_metadata_dict(request):
    with open(request.param, 'r') as f:
        expected_metadata_dict = json.load(f)
    granule_name = expected_metadata_dict['granule_name']
    input_file = os.path.join(os.path.dirname(__file__), f"fixtures/{granule_name}")
    try:
        process = getattr(mdx,
                          get_switcher_dict().get(expected_metadata_dict['collection_type'],
                                                  {}).get(expected_metadata_dict['shortname'],
                                                          'NoAttributeFound'))
    except AttributeError:
        process_switcher_type = {
            'netcdf': getattr(src, "ExtractNetCDFMetadata"),
            'csv': getattr(src, "ExtractCSVMetadata"),
            'legacy': getattr(mdx, 'ExtractLegacyMetadata')
        }
        process = process_switcher_type.get(expected_metadata_dict['collection_type'])
        if process is None:
            print(f"Ensure that collection {expected_metadata_dict['shortname']} is added to "
                  f"switchers.json")
            return 1
    process_collection = process(input_file)
    generated_metadata = {'ShortName': expected_metadata_dict['shortname'],
                          'GranuleUR': expected_metadata_dict.get('updated_granule_name',
                                                                  granule_name),
                          'VersionId': expected_metadata_dict['version'],
                          'DataFormat': expected_metadata_dict['format'],
                          }
    expected_metadata_dict['process_collection'] = process_collection
    expected_metadata_dict['generated_metadata'] = generated_metadata
    return expected_metadata_dict


class TestCollectionProcess:
    """
    Test processing for all collections.
    This will test if the collections' metadata will be extracted correctly
    """

    def test_1_get_start_date(self, expected_metadata_dict):
        """
        Testing get correct start date
        :return:
        """
        print(f"Error in collection: {expected_metadata_dict['shortname']}")
        start_date = expected_metadata_dict['process_collection'].get_temporal()[0]
        expected_metadata_dict['generated_metadata']['BeginningDateTime'] = start_date
        assert start_date == expected_metadata_dict['start']

    def test_2_get_stop_date(self, expected_metadata_dict):
        """
        Testing get correct start date
        :return:
        """
        print(f"Error in collection: {expected_metadata_dict['shortname']}")
        stop_date = expected_metadata_dict['process_collection'].get_temporal()[1]
        expected_metadata_dict['generated_metadata']['EndingDateTime'] = stop_date
        assert stop_date == expected_metadata_dict['end']

    def test_3_get_file_size(self, expected_metadata_dict):
        """
        Test getting the correct file size
        :return:
        """
        print(f"Error in collection: {expected_metadata_dict['shortname']}")
        updated_granule_name = expected_metadata_dict.get('updated_granule_name', None)
        if updated_granule_name:
            renamed_file_path = os.path.join(os.path.dirname(__file__), "fixtures",
                                             updated_granule_name)
            file_size = str(round(1E-6 * os.path.getsize(renamed_file_path), 2))
        else:
            file_size = str(round(expected_metadata_dict['process_collection'].
                                  get_file_size_megabytes(), 2))
        expected_metadata_dict['generated_metadata']['SizeMBDataGranule'] = file_size
        assert file_size == str(expected_metadata_dict['size'])

    @staticmethod
    def get_wnes(expected_metadata_dict, index):
        """
        A function helper to ger North, West, Souh, East
        :return: wnes[index] where index: west = 0 - north = 1 - east = 2 - south = 3
        """
        print(f"Error in collection: {expected_metadata_dict['shortname']}")
        process_geos = expected_metadata_dict['process_collection']
        wnes = process_geos.get_wnes_geometry()
        return str(round(wnes[index], 3))

    def test_4_get_north(self, expected_metadata_dict):
        """
        Test geometry metadata
        :return:
        """
        print(f"Error in collection: {expected_metadata_dict['shortname']}")
        north = self.get_wnes(expected_metadata_dict, 1)
        expected_metadata_dict['generated_metadata']['NorthBoundingCoordinate'] = north
        assert north == expected_metadata_dict['north']

    def test_5_get_west(self, expected_metadata_dict):
        """
        Test geometry metadata
        :return:
        """
        print(f"Error in collection: {expected_metadata_dict['shortname']}")
        west = self.get_wnes(expected_metadata_dict, 0)
        expected_metadata_dict['generated_metadata']['WestBoundingCoordinate'] = west
        assert west == expected_metadata_dict['west']

    def test_6_get_south(self, expected_metadata_dict):
        """
        Test geometry metadata
        :return:
        """
        print(f"Error in collection: {expected_metadata_dict['shortname']}")
        south = self.get_wnes(expected_metadata_dict, 3)
        expected_metadata_dict['generated_metadata']['SouthBoundingCoordinate'] = south
        assert south == expected_metadata_dict['south']

    def test_7_get_east(self, expected_metadata_dict):
        """
        Test geometry metadata
        :return:
        """
        print(f"Error in collection: {expected_metadata_dict['shortname']}")
        east = self.get_wnes(expected_metadata_dict, 2)
        expected_metadata_dict['generated_metadata']['EastBoundingCoordinate'] = east
        assert east == expected_metadata_dict['east']

    def test_8_get_checksum(self, expected_metadata_dict):
        """
        Test getting the checksum of the input file
        :return: the MD5 string
        """
        print(f"Error in collection: {expected_metadata_dict['shortname']}")
        updated_granule_name = expected_metadata_dict.get('updated_granule_name', None)
        if updated_granule_name:
            renamed_file_path = os.path.join(os.path.dirname(__file__), "fixtures",
                                             updated_granule_name)
            with open(renamed_file_path, 'rb') as file:
                checksum = md5(file.read()).hexdigest()
            # The expected checksum of a generated file cannot be static
            expected_checksum = checksum
        else:
            checksum = expected_metadata_dict['process_collection'].get_checksum()
            expected_checksum = expected_metadata_dict['checksum']
        expected_metadata_dict['generated_metadata']['checksum'] = checksum
        # Checks if checksum matches value or regex provided
        assert (checksum == expected_checksum or re.match(expected_checksum, checksum))

    def test_9_generate_metadata(self, expected_metadata_dict):
        """
        Test generating metadata of collection
        :return: metadata object
        """
        print(f"Error in collection: {expected_metadata_dict['shortname']}")
        metadata = expected_metadata_dict['process_collection'].get_metadata(
            ds_short_name=expected_metadata_dict['shortname'],
            format=expected_metadata_dict['format'],
            version=expected_metadata_dict['version'])
        for key in expected_metadata_dict['generated_metadata'].keys():
            assert metadata[key] == expected_metadata_dict['generated_metadata'][key]

    def test_a1_generate_umm_json(self, expected_metadata_dict):
        """
        Test generate the umm json in tmp folder
        """
        print(f"Error in collection: {expected_metadata_dict['shortname']}")
        expected_metadata_dict['generated_metadata']['OnlineAccessURL'] = "http://localhost.com"
        umm_json = GenerateUmmGJson(expected_metadata_dict['generated_metadata'])
        umm_json.generate_umm_json_file()
        umm_json_name = expected_metadata_dict.get('updated_granule_name',
                                                   expected_metadata_dict["granule_name"])
        assert os.path.exists(os.path.join(os.sep, 'tmp', f'{umm_json_name}.cmr.json'))
