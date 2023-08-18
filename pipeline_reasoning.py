from config import CONFIG_DIR
from eye import Eye
from lib_utils import append_task_result_file, loadTask, select_input_container_graph,fetch_path_from_input_container, updateStatus,write_ttl_file, append_task_result_graph
from constants import STATUS_FAILED, TASK_HARVESTING_REASONING, STATUS_BUSY
from pathlib import Path
from helpers import log,generate_uuid
import os
def runPipeline(entry): 
    task = loadTask(entry)
    if task is None or len(task.get('operation', '')) == 0 or task.get('operation', '') != TASK_HARVESTING_REASONING :
      return None
    updateStatus(task, STATUS_BUSY)
    input_container = select_input_container_graph(task)
    assert len(input_container) > 0
    input_container = input_container[0]
    path = fetch_path_from_input_container(input_container)
    assert path is not None and os.path.exists(path)
    config = Path(f"{CONFIG_DIR}")
    assert config.exists()
    eye = Eye()

    queries = [query.resolve() for query in config.glob("*.n3q")]
    if queries:
        eye.add_queries(queries)
    eye.add_data_by_reference([data.resolve() for data in config.glob("*.n3")])
    eye.add_data_by_path(path)
    data, code = eye.reason()
    if code:
        log(f"an error occurred")
        updateStatus(task, STATUS_FAILED)
    else:
        file_uri= write_ttl_file(task.get('graph',''), data, "enriched-data.ttl")
        file_container_uuid = generate_uuid()
        file_container_uri =f"http://redpencil.data.gift/id/dataContainers/{file_container_uuid}"
        append_task_result_file(task, file_container_uri, file_container_uuid, file_uri)
        graph_container_uuid = generate_uuid()
        graph_container_uri = f"http://redpencil.data.gift/id/dataContainers/{graph_container_uuid}"
        append_task_result_graph(task, graph_container_uri,graph_container_uuid, file_container_uri)
        updateStatus(task, STATUS_FAILED)





    

  
    
