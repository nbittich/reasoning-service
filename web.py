from pathlib import Path

from flask import Response, make_response, request, Flask
from helpers import error, log
from pipeline_reasoning import runPipeline
from constants import STATUS_SCHEDULED, TASK_HARVESTING_REASONING
from lib_utils import isTask

from eye import Eye
from config import CONFIG_DIR

app = Flask(__name__) # to be removed

def get_entries(deltas): 
    entries = []
    for delta in deltas:
        for insert in delta.inserts:
             predicate = insert.predicate.value
             obj = insert.object.value
             subject = insert.subject.value
             if predicate == 'http://www.w3.org/ns/adms#status' and obj == STATUS_SCHEDULED:
                entries.append(subject)
    return entries



@app.route("/delta", methods=["POST"])
def delta() -> Response:
    response = make_response()
    response.status = 204
    entries = get_entries(request.json)
    if len(entries) == 0:
        log('Delta did not contain potential tasks that are ready for import, awaiting the next batch!')
        return response

    for entry in entries:
        # todo i stopped there
        if isTask(entry):
          runPipeline(entry)

    return response


@app.route("/reason/", defaults={"path": None}, methods=["GET", "POST"])
@app.route("/reason/<path:path>", methods=["GET", "POST"])
def reason_with_config(path) -> Response:
    log(f"{request.method} {path}")
    if "data" not in request.values:
        msg = f"No data in to reason upon {path}"
        log(f"400 {msg}")
        return error(msg, status=400)
    eye = Eye()

    if path is not None:
        config = Path(f"{CONFIG_DIR}/{path}")
        if config.exists():
            queries = [query.resolve() for query in config.glob("*.n3q")]
            if queries:
                eye.add_queries(queries)
            eye.add_data_by_reference([data.resolve() for data in config.glob("*.n3")])
        else:
            msg = f"No config for {path}"
            log(f"404 {msg}")
            return error(msg, status=404)

    data_content = request.values["data"]
    assert isinstance(data_content, str)
    if data_content.startswith("http"):
        eye.add_data_by_reference(data_content.split(","))
    else:
        eye.add_data_by_value(data_content)

    data, code = eye.reason()

    if code:
        log(f"500 {path}")
        return error(data, status=500)
    else:
        response = make_response(data)
        response.mimetype = "text/turtle"
        log(f"200 {path}")
        return response




