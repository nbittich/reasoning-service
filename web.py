from pathlib import Path

from flask import Response, make_response, request
from helpers import error, log
from pipeline_reasoning import runPipeline
from constants import STATUS_SCHEDULED
from lib_utils import isTask

from eye import Eye
from config import CONFIG_DIR

def get_entries(deltas): 
    entries = []
    for delta in deltas:
        for insert in delta.get('inserts', []):
             predicate = insert.get('predicate').get('value')
             obj = insert.get('object').get('value')
             subject = insert.get('subject').get('value')
             if predicate == 'http://www.w3.org/ns/adms#status' and obj == STATUS_SCHEDULED:
                entries.append(subject)
    return entries



@app.route("/hello")
def hello():
    response = make_response()
    log("hello")
    response.status = 200
    return response

@app.route("/delta", methods=["POST"])
def delta() -> Response:
    log("receiving delta msg")
    response = make_response()
    response.status = 204
    entries = get_entries(request.json)
    if len(entries) == 0:
        log('Delta did not contain potential tasks that are ready for import, awaiting the next batch!')
        return response

    for entry in entries:
        if isTask(entry):
          runPipeline(entry)

    return response













































