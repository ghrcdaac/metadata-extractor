"""Lookup creator for ER-2 SSFR TC4 (er2ssfrtc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "er2ssfrtc4"
provider_path = "er2ssfrtc4/"

nav_lookup = {'TC4_ER2_SSFR_20070707.txt': {'start': '2007-07-07T16:00:00+00:00',
  'end': '2007-07-07T18:00:00+00:00',
  'north': 39.407270000000004,
  'south': 35.00793,
  'east': -119.89037,
  'west': -120.16331000000001},
 'TC4_ER2_SSFR_20070709.txt': {'start': '2007-07-09T17:00:00+00:00',
  'end': '2007-07-09T22:00:00+00:00',
  'north': 35.72599,
  'south': 22.957310000000003,
  'east': -112.16097,
  'west': -118.30096},
 'TC4_ER2_SSFR_20070717.txt': {'start': '2007-07-17T13:00:00+00:00',
  'end': '2007-07-17T17:00:00+00:00',
  'north': 9.32224,
  'south': 4.19334,
  'east': -81.29780000000001,
  'west': -84.50873},
 'TC4_ER2_SSFR_20070719.txt': {'start': '2007-07-19T12:00:00+00:00',
  'end': '2007-07-19T19:00:00+00:00',
  'north': 15.262070000000001,
  'south': 8.32558,
  'east': -77.34718000000001,
  'west': -87.79673000000001},
 'TC4_ER2_SSFR_20070722.txt': {'start': '2007-07-22T12:00:00+00:00',
  'end': '2007-07-22T18:00:00+00:00',
  'north': 7.782100000000001,
  'south': 6.421850000000001,
  'east': -82.09689,
  'west': -86.48833},
 'TC4_ER2_SSFR_20070724.txt': {'start': '2007-07-24T12:00:00+00:00',
  'end': '2007-07-24T19:00:00+00:00',
  'north': 10.641630000000001,
  'south': 5.828080000000001,
  'east': -84.09228,
  'west': -86.22345000000001},
 'TC4_ER2_SSFR_20070725.txt': {'start': '2007-07-25T14:00:00+00:00',
  'end': '2007-07-25T19:00:00+00:00',
  'north': 15.541360000000001,
  'south': 10.019020000000001,
  'east': -82.6517,
  'west': -85.99926},
 'TC4_ER2_SSFR_20070729.txt': {'start': '2007-07-29T12:00:00+00:00',
  'end': '2007-07-29T18:00:00+00:00',
  'north': 9.553640000000001,
  'south': -4.745410000000001,
  'east': -82.15405000000001,
  'west': -84.62443},
 'TC4_ER2_SSFR_20070731.txt': {'start': '2007-07-31T13:00:00+00:00',
  'end': '2007-07-31T18:00:00+00:00',
  'north': 10.07944,
  'south': 8.53672,
  'east': -82.98266000000001,
  'west': -84.60040000000001},
 'TC4_ER2_SSFR_20070803.txt': {'start': '2007-08-03T13:00:00+00:00',
  'end': '2007-08-03T19:00:00+00:00',
  'north': 12.48751,
  'south': 7.6717200000000005,
  'east': -82.21053,
  'west': -85.70057000000001},
 'TC4_ER2_SSFR_20070805.txt': {'start': '2007-08-05T13:00:00+00:00',
  'end': '2007-08-05T18:00:00+00:00',
  'north': 9.905550000000002,
  'south': 6.45241,
  'east': -79.71663000000001,
  'west': -83.97452000000001},
 'TC4_ER2_SSFR_20070806.txt': {'start': '2007-08-06T12:00:00+00:00',
  'end': '2007-08-06T19:00:00+00:00',
  'north': 8.06465,
  'south': -3.0003,
  'east': -87.42439,
  'west': -92.68462000000001},
 'TC4_ER2_SSFR_20070808.txt': {'start': '2007-08-08T12:00:00+00:00',
  'end': '2007-08-08T18:00:00+00:00',
  'north': 9.82332,
  'south': 6.503730000000001,
  'east': -82.75607000000001,
  'west': -85.72340000000001},
 'TC4_ER2_SSFR_20070809.txt': {'start': '2007-08-09T12:00:00+00:00',
  'end': '2007-08-09T19:00:00+00:00',
  'north': 34.32678,
  'south': 12.7438,
  'east': -86.61364,
  'west': -114.16563000000001}
}

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        self.file_type = 'ASCII'
        if filename in nav_lookup:
            metadata = nav_lookup[filename]
        else:
            raise KeyError(f"No nav lookup for file {filename}")
        for field in ['start', 'end']:
            metadata[field] = datetime.fromisoformat(metadata[field])
        metadata['format'] = self.file_type
        return metadata



    def main(self):
        # start_time = time.time()
        self.process_collection(short_name, provider_path)
        # elapsed_time = time.time() - start_time
        # print(f"Elapsed time in seconds: {elapsed_time}")
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')
