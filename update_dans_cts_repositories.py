import logging
import sys
from os import popen
from pymongo import MongoClient
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom.minidom import parseString
import xml.sax
import csv
import re

RE3DATA_API_REPOSITORIES = "curl -s https://www.re3data.org/api/v1/repositories"
RE3DATA_API_REPOSITORY = "curl -s https://www.re3data.org/api/v1/repository/{}"
CERTIFICATE_PATTERN = ".*(<r3d:certificate>other</r3d:certificate>).*"

logging.basicConfig(filename='dans_cts.log', encoding='utf-8', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%d/%m/%Y %I:%M:%S')

connection = MongoClient('localhost', 27017)
db = connection.dans_cts
repository_ids = []
certificates = {}
added, updated = 0, 0


class RepositoriesHandler(xml.sax.ContentHandler):

    def __init__(self):
        self.CurrentData = ""

    def startElement(self, tag, attributes):
        self.CurrentData = tag

    def endElement(self, tag):
        self.CurrentData = ""

    def characters(self, content):
        global repository_ids
        if self.CurrentData == "id":
            repository_ids.append(content)


def update_dans_cts_repositories(argv):
    """
    Queries from re3data repository data, enriches it with certification information and then compares it against dans_cts_repositories data.
    If different, updates the data in dans_cts_repositories (Mongo db).

        Arguments:
            argument 1  File containing additional certification info

        Example: python3 update_dans_cts_repositories.py CTS_INFO.csv
     """
    cts_info = sys.argv[1]
    get_certification_data(cts_info)
    get_repositories()
    update_repositories()


def get_repositories():
    logging.info("Started repository update")
    result = popen(RE3DATA_API_REPOSITORIES).read()
    xml.sax.parseString(result, RepositoriesHandler())


def update_repositories():
    global added, updated
    count = 0
    for repository_id in repository_ids:
        curl = RE3DATA_API_REPOSITORY.format(repository_id)
        re3_data = popen(curl).read()
        data = enrich(re3_data, repository_id) if certificates.get(repository_id) else re3_data
        if data:
            dans_data = get_dans_cts_document(repository_id)
            if data != dans_data:
                json_item = {"_id": '{}'.format(repository_id), "r3d:re3data": data}
                if dans_data == "NOT FOUND":
                    db['repositories'].insert_one(json_item)
                    logging.info("Added document with id %s" % repository_id)
                    added += 1
                else:
                    db['repositories'].replace_one({'_id': repository_id}, json_item)
                    logging.info("Updated document %s" % repository_id)
                    updated += 1
        count += 1

    logging.info("%s repositor%s read" % (count, "y" if count == 1 else "ies"))
    logging.info("%s repositor%s added" % (added, "y" if added == 1 else "ies"))
    logging.info("%s repositor%s updated" % (updated, "y" if updated == 1 else "ies"))


def enrich(re3_xml_str, repository_id):
    r = re.match(CERTIFICATE_PATTERN, re3_xml_str.replace('\n', ''))
    if r:
        re3data_certificate = r.group(1)
        re3_xml_str = re3_xml_str.replace(re3data_certificate, "%s" % make_certificate_node(repository_id), 1)
        return re3_xml_str
    else:
        logging.error("No CTS certificate element found in re3data record for %s" % repository_id)
        return ""


def make_certificate_node(repository_id):
    certificate = Element("certificate")
    SubElement(certificate, "certificateName").text = "CoreTrustSeal"
    SubElement(certificate, "startDate").text = certificates[repository_id][0]
    SubElement(certificate, "endDate").text = certificates[repository_id][1]
    SubElement(certificate, "certificationType").text = certificates[repository_id][2]
    certificate_xml = parseString(tostring(certificate, "utf-8")).childNodes[0].toprettyxml()
    return certificate_xml


def get_dans_cts_document(repository_id):
    entity = db['repositories'].find_one({'_id': repository_id})
    if not entity:
        return "NOT FOUND"
    else:
        return entity["r3d:re3data"]


def get_certification_data(cts_info):
    global certificates

    with open(cts_info) as csv_input:
        csv_reader = csv.reader(csv_input, delimiter=';')
        next(csv_reader, None)  # skip the headers
        for row in csv_reader:
            certificates[row[0]] = [row[2], row[3], row[4]]


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(update_dans_cts_repositories.__doc__)
    else:
        update_dans_cts_repositories(sys.argv[1:])
