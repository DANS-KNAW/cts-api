import xmltodict
from bottle import route, run, request, response
import requests
import csv
import re
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom.minidom import parseString

RE3DATA_API_REPOSITORIES = "https://www.re3data.org/api/beta/repositories"
RE3DATA_API_REPOSITORY = "https://www.re3data.org/api/v1/repository/{}"
RE3DATA_API_REPOSITORIES_CERTIFICATE = "https://www.re3data.org/api/beta/repositories?query=&certificates[]={}"
CERTIFICATE_PATTERN = ".*(<r3d:certificate>other</r3d:certificate>).*"

certificates = {}


@route('/repositories', method='GET')
def get_repositories():
    format = get_parameter_value(request.query_string, "format")
    if format != "" and format != "xml" and format != "json":
        return "Wrong format"
    certificate = get_parameter_value(request.query_string, "certificate")
    repositories = requests.get(RE3DATA_API_REPOSITORIES_CERTIFICATE.format(certificate)) if certificate != "" else requests.get(RE3DATA_API_REPOSITORIES)
    if format == "json":
        response.content_type = "application/json"
        data = xmltodict.parse(repositories.text)
    else:
        response.content_type = "text/xml"
        data = repositories
    return data


@route('/repository/:id', method='GET')
def get_repository(id):
    format = get_parameter_value(request.query_string, "format")
    if format != "" and format != "xml" and format != "json":
        return "Wrong format"

    re3_data = requests.get(RE3DATA_API_REPOSITORY.format(id)).text
    data = enrich(re3_data, id) if certificates.get(id) else re3_data
    if format == "json":
        response.content_type = "application/json"
        return xmltodict.parse(data)
    else:
        response.content_type = "text/xml"
        return data


def get_parameter_value(s, parameter):
    if s.find(parameter + "=") >= 0:
        ind = len(parameter) + 1 if s.startswith(parameter + "=") else s.find("&" + parameter + "=") + len(parameter) + 2
        if ind >= len(parameter) + 1:
            start = s[ind:]
            end_ind = start.find("&")
            if end_ind >= 0:
                value = start[:end_ind]
            else:
                value = start
        else:
            value = ""
    else:
        value = ""
    return value


def get_certification_data(cts_info):
    global certificates

    with open(cts_info) as csv_input:
        csv_reader = csv.reader(csv_input, delimiter=';')
        next(csv_reader, None)  # skip the headers
        for row in csv_reader:
            certificates[row[0]] = [row[2], row[3], row[4]]


def enrich(re3_xml, repository_id):
    r = re.match(CERTIFICATE_PATTERN, re3_xml.replace('\n', ''))
    if r:
        re3data_certificate = r.group(1)
        return re3_xml.replace(re3data_certificate, "%s" % make_certificate_node(repository_id), 1)
    else:
        return re3_xml


def make_certificate_node(repository_id):
    certificate = Element("certificate")
    SubElement(certificate, "certificateName").text = "CoreTrustSeal"
    SubElement(certificate, "startDate").text = certificates[repository_id][0]
    SubElement(certificate, "endDate").text = certificates[repository_id][1]
    SubElement(certificate, "certificationType").text = certificates[repository_id][2]
    certificate_xml = parseString(tostring(certificate, "utf-8")).childNodes[0].toprettyxml()
    return certificate_xml

get_certification_data("CTS_INFO.csv")
run(host='localhost', port=8080)

