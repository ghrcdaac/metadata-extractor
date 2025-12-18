import xml.etree.ElementTree as ET
import subprocess
from datetime import datetime, timedelta, UTC
import re


class GenerateEcho10XML:
    """
    Class to generate echo 10 XML Data
    """

    def __init__(self, data, age_off=None):
        """
        Initiate the GenerateECHO10XML instance
        :param data: a json object with metadata needed to generate an echo10 xml
        """
        self.data = data
        self.age_off = age_off

    def add_sub_tags(self, top_tag, tag_names):
        """
        A subtag will be added if it has a value
        :param top_tag: a tag that will contain subtags to be added
        :param tag_names: list of tag names that will be added
        :return: tag containing subtags
        """

        for ele in tag_names:
            if self.data[ele]:  # if the subtag has a value
                tagToAdd = ET.SubElement(top_tag, ele)
                tagToAdd.text = self.data[ele]

        return top_tag

    def generate_echo10_xml_data(self):
        """
        Generate an echo 10 xml file according to CMR schema
        :return: xml_data, granule_ur
        """
        Granule = ET.Element("Granule")
        data = self.data
        data['ProductionDateTime'] = data['LastUpdate'] = data[
            'InsertTime'] = datetime.now(UTC).strftime(
            '%Y-%m-%dT%H:%M:%SZ')  # Using UTC Time and time format required by CMR
        topList = ['GranuleUR', 'InsertTime', 'LastUpdate']
        if data.get('AgeOffFlag', False) and self.age_off:
            topList.append('DeleteTime')
            data['DeleteTime'] = datetime.strftime(
                datetime.strptime(data['LastUpdate'], '%Y-%m-%dT%H:%M:%SZ')
                + timedelta(days=float(re.search(r'(.*) days', self.age_off)[1])),
                '%Y-%m-%dT%H:%M:%SZ')
        top = self.add_sub_tags(top_tag=Granule, tag_names=topList)

        # =============Collection tag tag ========================#

        Collection = ET.Element("Collection")

        collList = ['ShortName', 'VersionId']  # Tags for collection list

        Collection = self.add_sub_tags(top_tag=Collection, tag_names=collList)

        top.append(Collection)

        # ===============Resctriction Data ====================#
        RestrictionFlag = ET.SubElement(top, "RestrictionFlag")
        RestrictionFlag.text = '0.0'

        RestrictionComment = ET.SubElement(top, "RestrictionComment")
        RestrictionComment.text = 'This product have full public access'

        # =============TDataGranule tag ========================#

        DataGranule = ET.Element("DataGranule")
        data['DayNightFlag'] = data.get('DayNightFlag', 'UNSPECIFIED')
        DataGranuleList = ['SizeMBDataGranule', 'DayNightFlag', 'ProductionDateTime']

        DataGranule = self.add_sub_tags(top_tag=DataGranule, tag_names=DataGranuleList)

        top.append(DataGranule)

        # =============Temporal tag ========================#
        Temporal = ET.Element("Temporal")
        RangeDateTime = ET.SubElement(Temporal, "RangeDateTime")
        temporalList = ["BeginningDateTime"]

        if self.data['EndingDateTime']:  # if it has stop data
            temporalList.append("EndingDateTime")

        self.add_sub_tags(top_tag=RangeDateTime, tag_names=temporalList)

        top.append(Temporal)

        # =============Spatial tag ========================#

        Spatial = ET.Element("Spatial")
        HorizontalSpatialDomain = ET.SubElement(Spatial, "HorizontalSpatialDomain")
        Geometry = ET.SubElement(HorizontalSpatialDomain, "Geometry")
        BoundingRectangle = ET.SubElement(Geometry, "BoundingRectangle")

        geomList = ['WestBoundingCoordinate', 'NorthBoundingCoordinate', 'EastBoundingCoordinate',
                    'SouthBoundingCoordinate']

        self.add_sub_tags(top_tag=BoundingRectangle, tag_names=geomList)

        top.append(Spatial)

        # =============Price tag ========================#

        Price = ET.Element("Price")
        Price.text = '0.0'

        top.append(Price)

        OnlineAccessURLs = ET.Element("OnlineAccessURLs")
        OnlineAccessURL = ET.SubElement(OnlineAccessURLs, "OnlineAccessURL")
        URL = ET.SubElement(OnlineAccessURL, "URL")
        URL.text = self.data['OnlineAccessURL']
        URLDescription = ET.SubElement(OnlineAccessURL, "URLDescription")
        URLDescription.text = "Files may be downloaded directly to your workstation from this link"

        top.append(OnlineAccessURLs)

        # ==============Orderable tag =====================#
        Orderable = ET.SubElement(top, "Orderable")
        Orderable.text = 'true'
        DataFormat = ET.SubElement(top, "DataFormat")
        DataFormat.text = self.data['DataFormat']
        return [ET.tostring(top, encoding='utf-8'), self.data['GranuleUR']]

    def generate_echo10_xml_file(self, output_folder='/tmp/'):
        """
        Genrate a physical xml file in the provided location
        :param output_folder: the folder where the echo 10 xml file will reside
        :return:
        """
        xml_data, filename = self.generate_echo10_xml_data()
        proc = subprocess.Popen(
            ['xmllint', '--format', '/dev/stdin'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, )
        (output, error_output) = proc.communicate(xml_data)
        with open('%s/%s.cmr.xml' % (output_folder.rstrip('/'), filename), 'wb') as the_file:
            the_file.write(output)
