"""Lookup creator for DC-8 CAS TC4 (dc8castc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "dc8castc4"
provider_path = "dc8castc4/"

nav_lookup = {'TC4_CAS_DC8_20070628_CAS.txt': {'start': '2007-06-28T21:20:00+00:00',
  'end': '2007-06-29T00:15:00+00:00',
  'north': 40.138952,
  'south': 38.451804,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAS_DC8_20070628_CIPPIP.txt': {'start': '2007-06-28T21:20:00+00:00',
  'end': '2007-06-29T00:15:00+00:00',
  'north': 40.138952,
  'south': 38.451804,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_CAS_DC8_20070630_CAS.txt': {'start': '2007-06-30T19:36:00+00:00',
  'end': '2007-06-30T23:20:00+00:00',
  'north': 44.426224,
  'south': 38.643756,
  'east': -121.488932,
  'west': -127.532152},
 'TC4_CAS_DC8_20070630_CIPPIP.txt': {'start': '2007-06-30T19:36:00+00:00',
  'end': '2007-06-30T23:20:00+00:00',
  'north': 44.426224,
  'south': 38.643756,
  'east': -121.488932,
  'west': -127.532152},
 'TC4_CAS_DC8_20070702_CAS.txt': {'start': '2007-07-02T18:26:00+00:00',
  'end': '2007-07-02T23:38:00+00:00',
  'north': 38.755728,
  'south': 31.694268,
  'east': -117.093988,
  'west': -125.133096},
 'TC4_CAS_DC8_20070702_CIPPIP.txt': {'start': '2007-07-02T18:26:00+00:00',
  'end': '2007-07-02T23:38:00+00:00',
  'north': 38.755728,
  'south': 31.694268,
  'east': -117.093988,
  'west': -125.133096},
 'TC4_CAS_DC8_20070717_CIPPIP.txt': {'start': '2007-07-17T13:00:00+00:00',
  'end': '2007-07-17T20:15:00+00:00',
  'north': 10.00803,
  'south': -3.376408,
  'east': -80.030766,
  'west': -84.826126},
 'TC4_CAS_DC8_20070721_CAS.txt': {'start': '2007-07-21T12:15:00+00:00',
  'end': '2007-07-21T20:10:00+00:00',
  'north': 12.294216,
  'south': 2.468834,
  'east': -72.433205,
  'west': -85.591049},
 'TC4_CAS_DC8_20070721_CIPPIP.txt': {'start': '2007-07-21T12:15:00+00:00',
  'end': '2007-07-21T20:10:00+00:00',
  'north': 12.294216,
  'south': 2.468834,
  'east': -72.433205,
  'west': -85.591049},
 'TC4_CAS_DC8_20070722_CAS.txt': {'start': '2007-07-22T13:00:00+00:00',
  'end': '2007-07-22T20:15:00+00:00',
  'north': 15.941677,
  'south': 5.919914,
  'east': -78.158283,
  'west': -85.819702},
 'TC4_CAS_DC8_20070722_CIPPIP.txt': {'start': '2007-07-22T13:00:00+00:00',
  'end': '2007-07-22T20:15:00+00:00',
  'north': 15.941677,
  'south': 5.919914,
  'east': -78.158283,
  'west': -85.819702},
 'TC4_CAS_DC8_20070724_CAS.txt': {'start': '2007-07-24T12:50:00+00:00',
  'end': '2007-07-24T16:20:00+00:00',
  'north': 10.007858,
  'south': 5.012512,
  'east': -84.144459,
  'west': -86.060028},
 'TC4_CAS_DC8_20070724_CIPPIP.txt': {'start': '2007-07-24T12:50:00+00:00',
  'end': '2007-07-24T16:20:00+00:00',
  'north': 10.007858,
  'south': 5.012512,
  'east': -84.144459,
  'west': -86.060028},
 'TC4_CAS_DC8_20070728_CAS.txt': {'start': '2007-07-28T12:50:00+00:00',
  'end': '2007-07-28T19:20:00+00:00',
  'north': 16.579227,
  'south': 8.705292,
  'east': -81.294193,
  'west': -88.79631},
 'TC4_CAS_DC8_20070728_CIPPIP.txt': {'start': '2007-07-28T12:50:00+00:00',
  'end': '2007-07-28T19:20:00+00:00',
  'north': 16.579227,
  'south': 8.705292,
  'east': -81.294193,
  'west': -88.79631},
 'TC4_CAS_DC8_20070729_CAS.txt': {'start': '2007-07-29T12:40:00+00:00',
  'end': '2007-07-29T20:10:00+00:00',
  'north': 10.585155,
  'south': -6.471634,
  'east': -78.218193,
  'west': -85.170479},
 'TC4_CAS_DC8_20070729_CIPPIP.txt': {'start': '2007-07-29T12:40:00+00:00',
  'end': '2007-07-29T20:10:00+00:00',
  'north': 10.585155,
  'south': -6.471634,
  'east': -78.218193,
  'west': -85.170479},
 'TC4_CAS_DC8_20070731_CAS.txt': {'start': '2007-07-31T12:40:00+00:00',
  'end': '2007-07-31T20:15:00+00:00',
  'north': 10.619144,
  'south': 7.928867,
  'east': -82.360039,
  'west': -89.509563},
 'TC4_CAS_DC8_20070731_CIPPIP.txt': {'start': '2007-07-31T12:40:00+00:00',
  'end': '2007-07-31T20:15:00+00:00',
  'north': 10.619144,
  'south': 7.928867,
  'east': -82.360039,
  'west': -89.509563},
 'TC4_CAS_DC8_20070803_CAS.txt': {'start': '2007-08-03T12:55:00+00:00',
  'end': '2007-08-03T19:40:00+00:00',
  'north': 13.876591,
  'south': 5.130959,
  'east': -80.040035,
  'west': -86.320438},
 'TC4_CAS_DC8_20070803_CIPPIP.txt': {'start': '2007-08-03T12:55:00+00:00',
  'end': '2007-08-03T19:40:00+00:00',
  'north': 13.876591,
  'south': 5.130959,
  'east': -80.040035,
  'west': -86.320438},
 'TC4_CAS_DC8_20070805_CAS.txt': {'start': '2007-08-05T12:22:00+00:00',
  'end': '2007-08-05T19:23:00+00:00',
  'north': 10.008888,
  'south': 5.282021,
  'east': -76.675987,
  'west': -84.434395},
 'TC4_CAS_DC8_20070805_CIPPIP.txt': {'start': '2007-08-05T12:22:00+00:00',
  'end': '2007-08-05T19:23:00+00:00',
  'north': 10.008888,
  'south': 5.282021,
  'east': -76.675987,
  'west': -84.434395},
 'TC4_CAS_DC8_20070806_CAS.txt': {'start': '2007-08-06T11:46:00+00:00',
  'end': '2007-08-06T19:04:00+00:00',
  'north': 10.011978,
  'south': -3.001328,
  'east': -84.131927,
  'west': -92.314682},
 'TC4_CAS_DC8_20070806_CIPPIP.txt': {'start': '2007-08-06T11:46:00+00:00',
  'end': '2007-08-06T19:04:00+00:00',
  'north': 10.011978,
  'south': -3.001328,
  'east': -84.131927,
  'west': -92.314682},
 'TC4_CAS_DC8_20070808_CAS.txt': {'start': '2007-08-08T12:04:00+00:00',
  'end': '2007-08-08T21:17:00+00:00',
  'north': 10.008202,
  'south': 1.433201,
  'east': -70.252419,
  'west': -85.174427},
 'TC4_CAS_DC8_20070808_CIPPIP.txt': {'start': '2007-08-08T12:04:00+00:00',
  'end': '2007-08-08T21:17:00+00:00',
  'north': 10.008202,
  'south': 1.433201,
  'east': -70.252419,
  'west': -85.174427}
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
