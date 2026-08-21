import re
import tempfile
from datetime import datetime
import botocore
import numpy as np
import icartt

from utils.mdx import MDX

short_name = "metnavaloft"
provider_path = "metnavaloft/"

class MDXProcessing(MDX):
    
    def __init__(self):
        super().__init__()


    def process(self, filename: str, file_obj_stream: botocore.response.StreamingBody) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        file_bytes = file_obj_stream.read()
        with tempfile.NamedTemporaryFile(suffix=".ict", delete=False) as tmp_file:
            tmp_file.write(file_bytes)
            tmp_path = tmp_file.name

        ict = icartt.Dataset(tmp_path)

        lat = ict.data['Latitude']
        lon = ict.data['Longitude']
        north, south, east, west = [np.nanmax(lat), np.nanmin(lat),
                                    np.nanmax(lon), np.nanmin(lon)]

        year = int(re.search(r'(\d{4})$', ict.missionName).group(1))
        timestamps = (
            np.datetime64(f"{year}-01-01") 
            + np.array(ict.data['Day_Of_Year'] - 1, dtype='timedelta64[D]') 
            + np.array(ict.data['Time_Start'], dtype='timedelta64[s]')
        )
        start_time = (np.min(timestamps)).astype(datetime)
        end_time = (np.max(timestamps)).astype(datetime)
        
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": "ICARTT"
        }


    def main(self):
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()