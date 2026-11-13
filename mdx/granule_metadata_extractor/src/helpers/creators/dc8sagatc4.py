"""Lookup creator for DC-8 SAGA TC4 (dc8sagatc4)."""
# for all future collections
from datetime import datetime
from utils.mdx import MDX

short_name = "dc8sagatc4"
provider_path = "dc8sagatc4/"

nav_lookup = {'TC4_SAGA_20070628.txt': {'start': '2007-06-28T21:28:53+00:00',
  'end': '2007-06-29T00:13:14+00:00',
  'north': 40.134652,
  'south': 38.590264,
  'east': -119.426824,
  'west': -126.366508},
 'TC4_SAGA_20070630.txt': {'start': '2007-06-30T19:40:06+00:00',
  'end': '2007-06-30T23:17:20+00:00',
  'north': 44.421924,
  'south': 38.759856,
  'east': -121.488932,
  'west': -127.508588},
 'TC4_SAGA_20070702.txt': {'start': '2007-07-02T18:44:26+00:00',
  'end': '2007-07-02T23:58:59+00:00',
  'north': 38.783764,
  'south': 31.694956,
  'east': -117.109984,
  'west': -125.117272},
 'TC4_SAGA_20070713.txt': {'start': '2007-07-13T15:08:35+00:00',
  'end': '2007-07-13T21:43:37+00:00',
  'north': 38.651962,
  'south': 9.73423,
  'east': -84.346676,
  'west': -121.400642},
 'TC4_SAGA_20070717.txt': {'start': '2007-07-17T13:00:03+00:00',
  'end': '2007-07-17T20:14:36+00:00',
  'north': 9.998074,
  'south': -3.376064,
  'east': -80.052052,
  'west': -84.82338},
 'TC4_SAGA_20070721.txt': {'start': '2007-07-21T12:16:43+00:00',
  'end': '2007-07-21T20:08:30+00:00',
  'north': 12.293701,
  'south': 2.49115,
  'east': -72.440414,
  'west': -85.590019},
 'TC4_SAGA_20070722.txt': {'start': '2007-07-22T13:01:32+00:00',
  'end': '2007-07-22T20:58:54+00:00',
  'north': 15.931377,
  'south': 5.929356,
  'east': -78.16721,
  'west': -85.81953},
 'TC4_SAGA_20070724.txt': {'start': '2007-07-24T12:52:25+00:00',
  'end': '2007-07-24T16:30:55+00:00',
  'north': 9.995842,
  'south': 5.022469,
  'east': -84.144974,
  'west': -86.048527},
 'TC4_SAGA_20070728.txt': {'start': '2007-07-28T12:51:11+00:00',
  'end': '2007-07-28T19:18:49+00:00',
  'north': 16.555023,
  'south': 8.706665,
  'east': -81.30003,
  'west': -88.794765},
 'TC4_SAGA_20070729.txt': {'start': '2007-07-29T12:42:31+00:00',
  'end': '2007-07-29T20:06:22+00:00',
  'north': 10.566788,
  'south': -6.455841,
  'east': -78.232784,
  'west': -85.169449},
 'TC4_SAGA_20070731.txt': {'start': '2007-07-31T12:40:18+00:00',
  'end': '2007-07-31T20:13:36+00:00',
  'north': 10.618629,
  'south': 7.934532,
  'east': -82.384415,
  'west': -89.509048},
 'TC4_SAGA_20070803.txt': {'start': '2007-08-03T12:53:00+00:00',
  'end': '2007-08-03T19:38:49+00:00',
  'north': 13.876076,
  'south': 5.131817,
  'east': -80.040035,
  'west': -86.318893},
 'TC4_SAGA_20070805.txt': {'start': '2007-08-05T12:22:28+00:00',
  'end': '2007-08-05T19:20:13+00:00',
  'north': 9.99773,
  'south': 5.292492,
  'east': -76.684399,
  'west': -84.415169},
 'TC4_SAGA_20070806.txt': {'start': '2007-08-06T11:45:51+00:00',
  'end': '2007-08-06T19:01:40+00:00',
  'north': 10.004597,
  'south': -3.000298,
  'east': -84.138279,
  'west': -92.304382},
 'TC4_SAGA_20070808.txt': {'start': '2007-08-08T12:03:58+00:00',
  'end': '2007-08-08T21:14:46+00:00',
  'north': 9.99567,
  'south': 1.449509,
  'east': -70.25259,
  'west': -85.162239},
 'TC4_SAGA_20070810.txt': {'start': '2007-08-10T14:48:36+00:00',
  'end': '2007-08-10T21:21:07+00:00',
  'north': 39.204884,
  'south': 9.963226,
  'east': -84.139309,
  'west': -121.350861},
 'TC4_SAGA_AERO_20070713.txt': {'start': '2007-07-13T15:38:00+00:00',
  'end': '2007-07-13T20:56:00+00:00',
  'north': 36.246815,
  'south': 10.616055,
  'east': -88.835793,
  'west': -119.256077},
 'TC4_SAGA_AERO_20070717.txt': {'start': '2007-07-17T13:39:00+00:00',
  'end': '2007-07-17T19:52:00+00:00',
  'north': 7.855568,
  'south': -3.362503,
  'east': -80.221481,
  'west': -84.762611},
 'TC4_SAGA_AERO_20070721.txt': {'start': '2007-07-21T12:43:00+00:00',
  'end': '2007-07-21T18:50:00+00:00',
  'north': 12.146587,
  'south': 2.855415,
  'east': -73.13942,
  'west': -85.586414},
 'TC4_SAGA_AERO_20070722.txt': {'start': '2007-07-22T13:15:00+00:00',
  'end': '2007-07-22T20:23:00+00:00',
  'north': 12.872887,
  'south': 6.10033,
  'east': -78.544693,
  'west': -85.751038},
 'TC4_SAGA_AERO_20070724.txt': {'start': '2007-07-24T13:12:00+00:00',
  'end': '2007-07-24T15:56:00+00:00',
  'north': 8.609161,
  'south': 5.083065,
  'east': -84.662361,
  'west': -85.982094},
 'TC4_SAGA_AERO_20070728.txt': {'start': '2007-07-28T13:05:00+00:00',
  'end': '2007-07-28T18:44:00+00:00',
  'north': 16.415806,
  'south': 8.960381,
  'east': -81.391354,
  'west': -88.269482},
 'TC4_SAGA_AERO_20070729.txt': {'start': '2007-07-29T13:12:00+00:00',
  'end': '2007-07-29T19:06:00+00:00',
  'north': 7.956505,
  'south': -6.26152,
  'east': -78.555336,
  'west': -85.057182},
 'TC4_SAGA_AERO_20070731.txt': {'start': '2007-07-31T13:14:00+00:00',
  'end': '2007-07-31T19:31:00+00:00',
  'north': 10.615368,
  'south': 7.971268,
  'east': -82.808247,
  'west': -89.425106},
 'TC4_SAGA_AERO_20070803.txt': {'start': '2007-08-03T13:17:00+00:00',
  'end': '2007-08-03T19:14:00+00:00',
  'north': 13.840027,
  'south': 5.160999,
  'east': -80.237446,
  'west': -86.221218},
 'TC4_SAGA_AERO_20070805.txt': {'start': '2007-08-05T12:36:30+00:00',
  'end': '2007-08-05T18:30:00+00:00',
  'north': 9.559479,
  'south': 5.352573,
  'east': -76.725082,
  'west': -83.017159},
 'TC4_SAGA_AERO_20070806.txt': {'start': '2007-08-06T11:58:30+00:00',
  'end': '2007-08-06T18:07:00+00:00',
  'north': 9.292889,
  'south': -3.000126,
  'east': -84.658756,
  'west': -92.088604},
 'TC4_SAGA_AERO_20070808.txt': {'start': '2007-08-08T12:25:00+00:00',
  'end': '2007-08-08T20:26:00+00:00',
  'north': 8.377075,
  'south': 1.560402,
  'east': -70.375156,
  'west': -85.135288},
 'TC4_SAGA_AERO_20070810.txt': {'start': '2007-08-10T15:23:00+00:00',
  'end': '2007-08-10T21:14:00+00:00',
  'north': 39.178448,
  'south': 13.284016,
  'east': -86.145172,
  'west': -120.800171}
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
