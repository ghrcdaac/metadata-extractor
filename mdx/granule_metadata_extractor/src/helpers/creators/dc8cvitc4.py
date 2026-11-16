"""Lookup creator for DC-8 CVI TC4 (dc8cvitc4)."""
from datetime import datetime
from utils.mdx import MDX

short_name = "dc8cvitc4"
provider_path = "dc8cvitc4/"

nav_lookup = {'TC4_CVI_DC8_20070630.txt': {'start': '2007-06-30T19:37:34+00:00',
  'end': '2007-06-30T23:07:45+00:00',
  'north': 44.426224,
  'south': 38.643756,
  'east': -121.639088,
  'west': -127.532152},
 'TC4_CVI_DC8_20070713.txt': {'start': '2007-07-13T15:09:34+00:00',
  'end': '2007-07-13T21:40:00+00:00',
  'north': 38.599949,
  'south': 9.726677,
  'east': -84.530697,
  'west': -121.400814},
 'TC4_CVI_DC8_20070717.txt': {'start': '2007-07-17T13:02:07+00:00',
  'end': '2007-07-17T20:14:05+00:00',
  'north': 9.978504,
  'south': -3.376579,
  'east': -80.030766,
  'west': -84.826298},
 'TC4_CVI_DC8_20070721.txt': {'start': '2007-07-21T12:18:53+00:00',
  'end': '2007-07-21T20:05:57+00:00',
  'north': 12.294216,
  'south': 2.468834,
  'east': -72.433205,
  'west': -85.591049},
 'TC4_CVI_DC8_20070722.txt': {'start': '2007-07-22T13:01:52+00:00',
  'end': '2007-07-22T20:49:39+00:00',
  'north': 15.941677,
  'south': 5.919914,
  'east': -78.158283,
  'west': -85.819702},
 'TC4_CVI_DC8_20070724.txt': {'start': '2007-07-24T12:54:53+00:00',
  'end': '2007-07-24T16:18:20+00:00',
  'north': 9.983311,
  'south': 5.012512,
  'east': -84.192352,
  'west': -86.060028},
 'TC4_CVI_DC8_20070728.txt': {'start': '2007-07-28T12:54:24+00:00',
  'end': '2007-07-28T19:12:46+00:00',
  'north': 16.579227,
  'south': 8.705292,
  'east': -81.294193,
  'west': -88.796482},
 'TC4_CVI_DC8_20070729.txt': {'start': '2007-07-29T12:45:10+00:00',
  'end': '2007-07-29T20:03:33+00:00',
  'north': 10.585155,
  'south': -6.471634,
  'east': -78.218193,
  'west': -85.170479},
 'TC4_CVI_DC8_20070731.txt': {'start': '2007-07-31T12:42:06+00:00',
  'end': '2007-07-31T20:09:08+00:00',
  'north': 10.619144,
  'south': 7.928867,
  'east': -82.360039,
  'west': -89.509563},
 'TC4_CVI_DC8_20070803.txt': {'start': '2007-08-03T12:56:08+00:00',
  'end': '2007-08-03T19:39:02+00:00',
  'north': 13.876762,
  'south': 5.130787,
  'east': -80.040035,
  'west': -86.32061},
 'TC4_CVI_DC8_20070805.txt': {'start': '2007-08-05T12:24:43+00:00',
  'end': '2007-08-05T19:17:37+00:00',
  'north': 9.952755,
  'south': 5.282021,
  'east': -76.675987,
  'west': -84.424095},
 'TC4_CVI_DC8_20070806.txt': {'start': '2007-08-06T11:48:49+00:00',
  'end': '2007-08-06T18:53:45+00:00',
  'north': 9.979534,
  'south': -3.001328,
  'east': -84.196301,
  'west': -92.314682},
 'TC4_CVI_DC8_20070808.txt': {'start': '2007-08-08T12:06:29+00:00',
  'end': '2007-08-08T21:11:09+00:00',
  'north': 9.983482,
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