from lib_utils import loadTask, select_input_container_graph,fetch_path_from_input_container, updateStatus
from constants import TASK_HARVESTING_REASONING, STATUS_BUSY
def runPipeline(entry): 
    task = loadTask(entry)
    if task is None or len(task.get('operation', '')) == 0 or task.get('operation', '') != TASK_HARVESTING_REASONING :
      return None
    updateStatus(task, STATUS_BUSY)
    input_container = select_input_container_graph(task)[0]
    path = fetch_path_from_input_container(input_container)
  
    
