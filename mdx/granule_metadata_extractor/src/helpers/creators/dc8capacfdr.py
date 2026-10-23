"""Creator for DC-8 CAPAC Ferry (dc8capacfdr)."""
from typing import Any
from datetime import datetime, timezone, timedelta
from utils.mdx import MDX


short_name = "dc8capacfdr"
provider_path = "dc8capacfdr/"

# Nav data not included in the file for the collection.
# Spatial bounds were pulled from the MMS data for the same flight (98225_MMS_DC8.txt).
nav_data = {
    "north": 28.929, 
    "south": 24.068, 
    "east": -77.283, 
    "west": -82.18
}


class MDXProcessing(MDX):
    def __init__(self):
        super().__init__()


    def process(self, filename: str, stream) -> dict[str, Any]:
        start_time = datetime.max.replace(tzinfo=timezone.utc)
        end_time = datetime.min.replace(tzinfo=timezone.utc)
        file_type = "ASCII"

        file_lines = []
        for line in stream.iter_lines():
            file_lines.append(line.decode("ascii"))
            
        num_comments = int(file_lines[0].split()[0])
        date_str = '-'.join(file_lines[2].split()[:3])
        date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        # Exclude trailing DOS EOF char
        for line in file_lines[num_comments:-1]:
            utc_secs = int(line.split()[1])
            timestamp = date + timedelta(seconds=utc_secs)
            start_time = min(start_time, timestamp)
            end_time = max(end_time, timestamp)

        return {
            "start": start_time,
            "end": end_time,
            "north": nav_data["north"],
            "south": nav_data["south"],
            "east": nav_data["east"],
            "west": nav_data["west"],
            "format": file_type
        }


    def main(self):
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()