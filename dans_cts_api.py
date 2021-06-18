import xmltodict
from bottle import route, run, request, abort, response
from pymongo import MongoClient
import requests

RE3DATA_API_REPOSITORIES = "https://www.re3data.org/api/beta/repositories"
RE3DATA_API_REPOSITORIES_CERTIFICATE = "https://www.re3data.org/api/beta/repositories?query=&certificates[]={}"

connection = MongoClient('localhost', 27017)
db = connection.dans_cts


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

    entity = db['repositories'].find_one({'_id': id})
    if not entity:
        abort(404, 'No document with id %s' % id)

    if format == "json":
        response.content_type = "application/json"
        data = xmltodict.parse(entity["r3d:re3data"])
    else:
        response.content_type = "text/xml"
        data = entity["r3d:re3data"]
    return data


def get_parameter_value(s, parameter):
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
    return value


run(host='localhost', port=8080)

