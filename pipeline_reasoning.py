from config import REASONING_CONFIG_FILE_NAME, CONFIG_DIR
from eye import Eye
from lib_utils import loadTask, select_input_container_graph,fetch_path_from_input_container, updateStatus
from constants import TASK_HARVESTING_REASONING, STATUS_BUSY
from pathlib import Path

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


    

  
    
