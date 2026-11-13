"""Lookup creator for DC-8 FASTOZ TC4 (dc8fastoztc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "dc8fastoztc4"
provider_path = "dc8fastoztc4/"

nav_lookup = {'TC4_FASTOZ_20070628.txt': {'start': '2007-06-28T21:30:30+00:00',
  'end': '2007-06-29T00:16:26+00:00',
  'north': 40.138952,
  'south': 38.44974,
  'east': -119.374708,
  'west': -126.368228},
 'TC4_FASTOZ_20070630.txt': {'start': '2007-06-30T19:36:09+00:00',
  'end': '2007-06-30T23:19:59+00:00',
  'north': 44.426224,
  'south': 38.643756,
  'east': -121.488932,
  'west': -127.532152},
 'TC4_FASTOZ_20070702.txt': {'start': '2007-07-02T18:42:59+00:00',
  'end': '2007-07-03T00:00:22+00:00',
  'north': 38.794256,
  'south': 31.694268,
  'east': -117.093816,
  'west': -125.133096},
 'TC4_FASTOZ_20070713.txt': {'start': '2007-07-13T15:08:24+00:00',
  'end': '2007-07-13T21:46:43+00:00',
  'north': 38.660374,
  'south': 9.726677,
  'east': -84.217415,
  'west': -121.400814},
 'TC4_FASTOZ_20070717.txt': {'start': '2007-07-17T12:59:58+00:00',
  'end': '2007-07-17T20:16:46+00:00',
  'north': 10.00803,
  'south': -3.376579,
  'east': -80.030766,
  'west': -84.826298},
 'TC4_FASTOZ_20070721.txt': {'start': '2007-07-21T12:16:47+00:00',
  'end': '2007-07-21T19:45:47+00:00',
  'north': 12.294216,
  'south': 2.468834,
  'east': -72.433205,
  'west': -85.591049},
 'TC4_FASTOZ_20070722.txt': {'start': '2007-07-22T13:00:17+00:00',
  'end': '2007-07-22T21:00:53+00:00',
  'north': 15.941677,
  'south': 5.919914,
  'east': -78.158283,
  'west': -85.819702},
 'TC4_FASTOZ_20070724.txt': {'start': '2007-07-24T12:52:30+00:00',
  'end': '2007-07-24T16:33:00+00:00',
  'north': 10.007858,
  'south': 5.012512,
  'east': -84.144459,
  'west': -86.060028},
 'TC4_FASTOZ_20070728.txt': {'start': '2007-07-28T12:51:16+00:00',
  'end': '2007-07-28T19:21:45+00:00',
  'north': 16.579227,
  'south': 8.705292,
  'east': -81.294193,
  'west': -88.796482},
 'TC4_FASTOZ_20070729.txt': {'start': '2007-07-29T12:42:32+00:00',
  'end': '2007-07-29T20:09:21+00:00',
  'north': 10.585155,
  'south': -6.471634,
  'east': -78.218193,
  'west': -85.170479},
 'TC4_FASTOZ_20070731.txt': {'start': '2007-07-31T12:40:30+00:00',
  'end': '2007-07-31T20:15:24+00:00',
  'north': 10.619144,
  'south': 7.928867,
  'east': -82.360039,
  'west': -89.509563},
 'TC4_FASTOZ_20070803.txt': {'start': '2007-08-03T12:53:22+00:00',
  'end': '2007-08-03T19:41:28+00:00',
  'north': 13.876762,
  'south': 5.130787,
  'east': -80.040035,
  'west': -86.32061},
 'TC4_FASTOZ_20070805.txt': {'start': '2007-08-05T12:22:27+00:00',
  'end': '2007-08-05T19:22:58+00:00',
  'north': 10.00906,
  'south': 5.282021,
  'east': -76.675987,
  'west': -84.434395},
 'TC4_FASTOZ_20070806.txt': {'start': '2007-08-06T11:45:59+00:00',
  'end': '2007-08-06T19:03:40+00:00',
  'north': 10.01215,
  'south': -3.001328,
  'east': -84.131927,
  'west': -92.314682},
 'TC4_FASTOZ_20070808.txt': {'start': '2007-08-08T12:04:18+00:00',
  'end': '2007-08-08T21:17:05+00:00',
  'north': 10.008202,
  'south': 1.433201,
  'east': -70.252419,
  'west': -85.174427},
 'TC4_FASTOZ_20070810.txt': {'start': '2007-08-10T14:50:39+00:00',
  'end': '2007-08-10T21:23:18+00:00',
  'north': 39.2169,
  'south': 9.953098,
  'east': -84.144802,
  'west': -121.400471}
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
