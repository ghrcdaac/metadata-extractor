"""Lookup creator for DC-8 DLH TC4 (dc8dlhtc4)."""
from datetime import datetime
from utils.mdx import MDX

short_name = "dc8dlhtc4"
provider_path = "dc8dlhtc4/"

nav_lookup = {
	"TC4_DLH_H2O_20070808_R1.txt": {
		"start": "2007-08-08T12:03:25+00:00",
		"end": "2007-08-08T21:17:20+00:00",
		"north": 10.008202,
		"south": 1.433201,
		"east": -70.252419,
		"west": -85.174427
	},
	"TC4_DLH_CO_20070630_R1.txt": {
		"start": "2007-06-30T19:35:55+00:00",
		"end": "2007-06-30T23:19:52+00:00",
		"north": 44.426224,
		"south": 38.643756,
		"east": -121.488932,
		"west": -127.532152
	},
	"TC4_DLH_CO_20070731_R1.txt": {
		"start": "2007-07-31T12:40:10+00:00",
		"end": "2007-07-31T20:16:04+00:00",
		"north": 10.619144,
		"south": 7.928867,
		"east": -82.360039,
		"west": -89.509563
	},
	"TC4_DLH_CO_20070702_R1.txt": {
		"start": "2007-07-02T18:42:59+00:00",
		"end": "2007-07-03T00:01:06+00:00",
		"north": 38.794256,
		"south": 31.694268,
		"east": -117.093816,
		"west": -125.133096
	},
	"TC4_DLH_CO_20070722_R1.txt": {
		"start": "2007-07-22T13:00:18+00:00",
		"end": "2007-07-22T21:00:07+00:00",
		"north": 15.941677,
		"south": 5.919914,
		"east": -78.158283,
		"west": -85.819702
	},
	"TC4_DLH_H2O_20070630_R1.txt": {
		"start": "2007-06-30T19:35:47+00:00",
		"end": "2007-06-30T23:21:03+00:00",
		"north": 44.426224,
		"south": 38.643756,
		"east": -121.488932,
		"west": -127.532152
	},
	"TC4_DLH_H2O_20070724_R1.txt": {
		"start": "2007-07-24T12:52:21+00:00",
		"end": "2007-07-24T16:33:05+00:00",
		"north": 10.007858,
		"south": 5.012512,
		"east": -84.144459,
		"west": -86.060028
	},
	"TC4_DLH_H2O_20070803_R1.txt": {
		"start": "2007-08-03T12:52:52+00:00",
		"end": "2007-08-03T19:41:45+00:00",
		"north": 13.876762,
		"south": 5.130787,
		"east": -80.040035,
		"west": -86.32061
	},
	"TC4_DLH_H2O_20070728_R1.txt": {
		"start": "2007-07-28T12:51:07+00:00",
		"end": "2007-07-28T19:21:49+00:00",
		"north": 16.579227,
		"south": 8.705292,
		"east": -81.294193,
		"west": -88.796482
	},
	"TC4_DLH_H2O_20070806_R1.txt": {
		"start": "2007-08-06T11:45:31+00:00",
		"end": "2007-08-06T19:03:50+00:00",
		"north": 10.01215,
		"south": -3.001328,
		"east": -84.131927,
		"west": -92.314682
	},
	"TC4_DLH_H2O_20070810_R1.txt": {
		"start": "2007-08-10T14:48:35+00:00",
		"end": "2007-08-10T21:23:24+00:00",
		"north": 39.2169,
		"south": 9.953098,
		"east": -84.136219,
		"west": -121.400471
	},
	"TC4_DLH_CO_20070808_R1.txt": {
		"start": "2007-08-08T12:03:25+00:00",
		"end": "2007-08-08T21:16:54+00:00",
		"north": 10.008202,
		"south": 1.433201,
		"east": -70.252419,
		"west": -85.174427
	},
	"TC4_DLH_H2O_20070729_R1.txt": {
		"start": "2007-07-29T12:42:10+00:00",
		"end": "2007-07-29T20:09:30+00:00",
		"north": 10.585155,
		"south": -6.471634,
		"east": -78.218193,
		"west": -85.170479
	},
	"TC4_DLH_CO_20070806_R1.txt": {
		"start": "2007-08-06T11:45:46+00:00",
		"end": "2007-08-06T19:03:37+00:00",
		"north": 10.01215,
		"south": -3.001328,
		"east": -84.131927,
		"west": -92.314682
	},
	"TC4_DLH_H2O_20070628_R1.txt": {
		"start": "2007-06-28T21:34:57+00:00",
		"end": "2007-06-29T00:15:38+00:00",
		"north": 40.138952,
		"south": 38.44974,
		"east": -119.374708,
		"west": -126.368228
	},
	"TC4_DLH_CO_20070717_R1.txt": {
		"start": "2007-07-17T13:05:25+00:00",
		"end": "2007-07-17T20:16:19+00:00",
		"north": 9.983997,
		"south": -3.376579,
		"east": -80.030766,
		"west": -84.826298
	},
	"TC4_DLH_H2O_20070721_R1.txt": {
		"start": "2007-07-21T12:16:14+00:00",
		"end": "2007-07-21T20:12:16+00:00",
		"north": 12.294216,
		"south": 2.468834,
		"east": -72.433205,
		"west": -85.591049
	},
	"TC4_DLH_H2O_20070717_R1.txt": {
		"start": "2007-07-17T12:59:20+00:00",
		"end": "2007-07-17T20:17:28+00:00",
		"north": 10.00803,
		"south": -3.376579,
		"east": -80.030766,
		"west": -84.826298
	},
	"TC4_DLH_H2O_20070702_R1.txt": {
		"start": "2007-07-02T18:42:31+00:00",
		"end": "2007-07-03T00:00:40+00:00",
		"north": 38.794256,
		"south": 31.694268,
		"east": -117.093816,
		"west": -125.133096
	},
	"TC4_DLH_CO_20070713_R1.txt": {
		"start": "2007-07-13T15:19:46+00:00",
		"end": "2007-07-13T21:47:14+00:00",
		"north": 38.065052,
		"south": 9.726677,
		"east": -84.203854,
		"west": -120.519505
	},
	"TC4_DLH_CO_20070728_R1.txt": {
		"start": "2007-07-28T12:51:02+00:00",
		"end": "2007-07-28T19:21:59+00:00",
		"north": 16.579227,
		"south": 8.705292,
		"east": -81.294193,
		"west": -88.796482
	},
	"TC4_DLH_CO_20070628_R1.txt": {
		"start": "2007-06-28T21:34:57+00:00",
		"end": "2007-06-29T00:13:05+00:00",
		"north": 40.138952,
		"south": 38.44974,
		"east": -119.374708,
		"west": -126.368228
	},
	"TC4_DLH_H2O_20070713_R1.txt": {
		"start": "2007-07-13T15:07:48+00:00",
		"end": "2007-07-13T21:47:03+00:00",
		"north": 38.679943,
		"south": 9.726677,
		"east": -84.206772,
		"west": -121.400814
	},
	"TC4_DLH_H2O_20070805_R1.txt": {
		"start": "2007-08-05T12:22:11+00:00",
		"end": "2007-08-05T19:23:00+00:00",
		"north": 10.00906,
		"south": 5.282021,
		"east": -76.675987,
		"west": -84.434395
	},
	"TC4_DLH_CO_20070724_R1.txt": {
		"start": "2007-07-24T12:51:51+00:00",
		"end": "2007-07-24T16:32:59+00:00",
		"north": 10.007858,
		"south": 5.012512,
		"east": -84.144459,
		"west": -86.060028
	},
	"TC4_DLH_CO_20070810_R1.txt": {
		"start": "2007-08-10T14:48:37+00:00",
		"end": "2007-08-10T21:23:18+00:00",
		"north": 39.2169,
		"south": 9.953098,
		"east": -84.136219,
		"west": -121.400471
	},
	"TC4_DLH_H2O_20070722_R1.txt": {
		"start": "2007-07-22T12:59:37+00:00",
		"end": "2007-07-22T21:01:26+00:00",
		"north": 15.941677,
		"south": 5.919914,
		"east": -78.158283,
		"west": -85.819702
	},
	"TC4_DLH_CO_20070803_R1.txt": {
		"start": "2007-08-03T12:52:39+00:00",
		"end": "2007-08-03T19:39:36+00:00",
		"north": 13.876762,
		"south": 5.130787,
		"east": -80.040035,
		"west": -86.32061
	},
	"TC4_DLH_CO_20070805_R1.txt": {
		"start": "2007-08-05T12:22:17+00:00",
		"end": "2007-08-05T19:22:47+00:00",
		"north": 10.00906,
		"south": 5.282021,
		"east": -76.675987,
		"west": -84.434395
	},
	"TC4_DLH_CO_20070729_R1.txt": {
		"start": "2007-07-29T12:42:02+00:00",
		"end": "2007-07-29T20:09:27+00:00",
		"north": 10.585155,
		"south": -6.471634,
		"east": -78.218193,
		"west": -85.170479
	},
	"TC4_DLH_CO_20070721_R1.txt": {
		"start": "2007-07-21T12:16:25+00:00",
		"end": "2007-07-21T19:59:22+00:00",
		"north": 12.294216,
		"south": 2.468834,
		"east": -72.433205,
		"west": -85.591049
	},
	"TC4_DLH_H2O_20070731_R1.txt": {
		"start": "2007-07-31T12:40:01+00:00",
		"end": "2007-07-31T20:15:30+00:00",
		"north": 10.619144,
		"south": 7.928867,
		"east": -82.360039,
		"west": -89.509563
	}
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
        self.process_collection(short_name, provider_path)
        # self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()