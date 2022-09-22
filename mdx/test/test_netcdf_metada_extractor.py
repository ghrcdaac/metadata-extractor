from os import path
import json
from unittest import TestCase
from granule_metadata_extractor.processing.process_goesrpltcrs import ExtractGeosrpltcrsMetadata
import sys

sys.path.insert(0, path.join(path.dirname(__file__), '..'))
from process_mdx.main import MDX


class TestProcessGeoER(TestCase):
    """
    Test processing Geosr.
    This will test if GlobaliR is processed correctly reguardless if this module is used in the cloud of on prem
    Testing the connections to AWS resources like S3 is assumed to be done by Cumulus
    """
    event = path.join(path.dirname(__file__), 'payload.json')
    input_files = [
        path.join(path.dirname(__file__), "fixtures/GOESR_CRS_L1B_20170507_v0.nc")
    ]
    with open(event) as f:
        payload = json.loads(f.read())
    config = payload['config']
    collection = config['collection']
    process = MDX(input=payload.get('input'), config=payload.get('config'))

    def test_1_get_bucket(self):
        """
        Testing get bucket to insure it returns the correct value
        :return:
        """
        files = self.collection['files']
        file_name = self.input_files[0].split('/')[-1]
        buckets = self.config.get('buckets')
        bucket = self.process.get_bucket(file_name, files, buckets)
        bucket_destination = bucket.get('name')
        self.assertTrue(bucket_destination == "ghrcsbxw-protected")

    def test_2_start_stop_date_goesrpltcrs(self):
        """
        Test start date and stop date
        :return:
        """
        file_path = self.input_files[0]
        exnet = ExtractGeosrpltcrsMetadata(file_path)
        units_variable = 'units'
        start, stop = exnet.get_temporal(
            units_variable=units_variable)  # get_temporal(units_variable=units_variable)
        self.assertEqual(start, "2017-05-07T16:08:45Z")
        self.assertEqual(stop, "2017-05-07T16:08:45Z")

    def test_3_geometry_goesrpltcrs(self):
        """
        Test geometry metadata
        :return:
        """
        file_path = self.input_files[0]
        exnet = ExtractGeosrpltcrsMetadata(file_path)
        wnes = exnet.get_wnes_geometry()
        self.assertListEqual(wnes, [-84.014, 30.039, -84.014, 30.039])
