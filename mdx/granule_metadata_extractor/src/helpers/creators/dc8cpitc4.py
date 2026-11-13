"""Lookup creator for DC-8 CPI TC4 (dc8cpitc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "dc8cpitc4"
provider_path = "dc8cpitc4/"

nav_lookup = {
 'TC4_CPI_DC8_CP_20070628.txt': {'start': '2007-06-28T21:23:05+00:00',
  'end': '2007-06-28T23:59:45+00:00',
  'north': 40.138952,
  'south': 38.455932,
  'east': -119.375052,
  'west': -126.368056},
 'TC4_CPI_DC8_CP_20070630.txt': {'start': '2007-06-30T19:39:59+00:00',
  'end': '2007-06-30T23:03:49+00:00',
  'north': 44.426052,
  'south': 38.751428,
  'east': -121.790448,
  'west': -127.531808},
 'TC4_CPI_DC8_CP_20070702.txt': {'start': '2007-07-02T18:46:30+00:00',
  'end': '2007-07-02T23:29:20+00:00',
  'north': 38.527484,
  'south': 31.694268,
  'east': -117.09416,
  'west': -125.133096},
 'TC4_CPI_DC8_CP_20070713.txt': {'start': '2007-07-13T14:55:00+00:00',
  'end': '2007-07-13T21:48:20+00:00',
  'north': 38.679428,
  'south': 9.726849,
  'east': -84.204712,
  'west': -121.400814},
 'TC4_CPI_DC8_CP_20070717.txt': {'start': '2007-07-17T12:45:30+00:00',
  'end': '2007-07-17T20:17:40+00:00',
  'north': 10.00803,
  'south': -3.376408,
  'east': -80.030937,
  'west': -84.826126},
 'TC4_CPI_DC8_CP_20070721.txt': {'start': '2007-07-21T11:53:00+00:00',
  'end': '2007-07-21T20:12:10+00:00',
  'north': 12.294044,
  'south': 2.468834,
  'east': -72.433205,
  'west': -85.591049},
 'TC4_CPI_DC8_CP_20070722.txt': {'start': '2007-07-22T12:51:20+00:00',
  'end': '2007-07-22T20:50:40+00:00',
  'north': 15.941677,
  'south': 5.920086,
  'east': -78.158283,
  'west': -85.819702},
 'TC4_CPI_DC8_CP_20070724.txt': {'start': '2007-07-24T12:42:00+00:00',
  'end': '2007-07-24T16:34:00+00:00',
  'north': 10.007858,
  'south': 5.012684,
  'east': -84.14463,
  'west': -86.059856},
 'TC4_CPI_DC8_CP_20070728.txt': {'start': '2007-07-28T14:44:20+00:00',
  'end': '2007-07-28T18:44:40+00:00',
  'north': 16.579227,
  'south': 8.705292,
  'east': -81.440964,
  'west': -88.79631},
 'TC4_CPI_DC8_CP_20070729.txt': {'start': '2007-07-29T12:31:50+00:00',
  'end': '2007-07-29T20:10:20+00:00',
  'north': 10.584812,
  'south': -6.471634,
  'east': -78.218193,
  'west': -85.170479},
 'TC4_CPI_DC8_CP_20070731.txt': {'start': '2007-07-31T12:13:50+00:00',
  'end': '2007-07-31T20:14:00+00:00',
  'north': 10.619144,
  'south': 7.929039,
  'east': -82.36021,
  'west': -89.509563},
 'TC4_CPI_DC8_CP_20070803.txt': {'start': '2007-08-03T12:41:50+00:00',
  'end': '2007-08-03T19:42:00+00:00',
  'north': 13.876591,
  'south': 5.130959,
  'east': -80.040035,
  'west': -86.320438},
 'TC4_CPI_DC8_CP_20070805.txt': {'start': '2007-08-05T12:17:20+00:00',
  'end': '2007-08-05T19:20:30+00:00',
  'north': 10.008888,
  'south': 5.282192,
  'east': -76.676159,
  'west': -84.434052},
 'TC4_CPI_DC8_CP_20070806.txt': {'start': '2007-08-06T11:37:00+00:00',
  'end': '2007-08-06T19:04:00+00:00',
  'north': 10.011978,
  'south': -3.001156,
  'east': -84.132099,
  'west': -92.314682},
 'TC4_CPI_DC8_CP_20070808.txt': {'start': '2007-08-08T11:54:40+00:00',
  'end': '2007-08-08T21:11:10+00:00',
  'north': 10.008202,
  'south': 1.433887,
  'east': -70.252934,
  'west': -85.174255},
 'TC4_CPI_DC8_CP_20070810.txt': {'start': '2007-08-10T14:37:10+00:00',
  'end': '2007-08-10T21:24:20+00:00',
  'north': 39.2169,
  'south': 9.953098,
  'east': -84.136391,
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
