import os
from string import Template
from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string, sparql_escape_int
from sudo_query_helpers import query_sudo, update_sudo
from datetime import datetime
from helpers import log,generate_uuid
import shutil
def isTask(deltaEntry):
    print("enter isTask")
    query_template = Template("""
        PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
        PREFIX terms: <http://purl.org/dc/terms/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
        PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX oslc: <http://open-services.net/ns/core#>
        PREFIX cogs: <http://vocab.deri.ie/cogs#>
        PREFIX adms: <http://www.w3.org/ns/adms#>

        ASK {
            GRAPH ?g {
             $entry a task:Task.
            }
        }
    """)
    query_string = query_template.substitute(entry=sparql_escape_uri(deltaEntry))
    query_result = query_sudo(query_string)
    return query_result.get('boolean') == True

def loadTask(entry) :
    print("enter loadTask")
    query_template = Template("""
                PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
                PREFIX terms: <http://purl.org/dc/terms/>
                PREFIX prov: <http://www.w3.org/ns/prov#>
                PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
                PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
                PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
                PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
                PREFIX dct: <http://purl.org/dc/terms/>
                PREFIX oslc: <http://open-services.net/ns/core#>
                PREFIX cogs: <http://vocab.deri.ie/cogs#>
                PREFIX adms: <http://www.w3.org/ns/adms#>

                SELECT DISTINCT ?graph ?task ?id ?job ?created ?modified ?status ?index ?operation ?error WHERE {
                      GRAPH ?graph {
                            BIND($task as ?task)
                            ?task a task:Task.
                            ?task dct:isPartOf ?job;
                                      mu:uuid ?id;
                                      dct:created ?created;
                                      dct:modified ?modified;
                                      adms:status ?status;
                                      task:index ?index;
                                      task:operation ?operation.
                                      OPTIONAL { ?task task:error ?error. }
                      }
                }
    """)
    query_string = query_template.substitute(task=sparql_escape_uri(entry))
    query_result = query_sudo(query_string)

    if len(query_result.get('results').get('bindings')) >= 1:
        task_res = query_result.get('results').get('bindings')[0]
        return {
            "task": task_res.get('task').get('value'),
            "job": task_res.get('job').get('value'),
            "id": task_res.get('id').get('value'),
            "created": task_res.get('created').get('value'),
            "modified": task_res.get('modified').get('value'),
            "operation": task_res.get('operation').get('value'),
            "index": task_res.get('index').get('value'),
            "graph": task_res.get('graph').get('value'),
            "status": task_res.get('status').get('value'),
        }
    else:
        return {}

def updateStatus(task, status):
    query_template = Template("""
                PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
                PREFIX adms: <http://www.w3.org/ns/adms#>
                PREFIX dct: <http://purl.org/dc/terms/>
                PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                DELETE {
                  GRAPH ?g {
                    ?subject adms:status ?status .
                    ?subject dct:modified ?modified.
                  }
                }
                INSERT {
                  GRAPH ?g {
                   ?subject adms:status $status.
                   ?subject dct:modified $date.
                  }
                }
                WHERE {
                  GRAPH ?g {
                    BIND($task as ?subject)
                    ?subject adms:status ?status .
                    OPTIONAL { ?subject dct:modified ?modified. }
                  }
                }
    """)
    now = sparql_escape_datetime(datetime.now())
    taskUri = sparql_escape_uri(task.get('task'))
    query_string = query_template.substitute(task=taskUri, status=sparql_escape_uri(status), date=now )
    update_sudo(query_string)

def select_input_container_graph(task):
    query_template = Template("""
      PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
      PREFIX terms: <http://purl.org/dc/terms/>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX oslc: <http://open-services.net/ns/core#>
      PREFIX cogs: <http://vocab.deri.ie/cogs#>
      PREFIX adms: <http://www.w3.org/ns/adms#>

      SELECT DISTINCT ?graph  WHERE {
            GRAPH ?g {
              $task task:inputContainer ?container.
              ?container task:hasGraph ?graph.
            }
      }
    """)
    taskUri = sparql_escape_uri(task.get('task'))
    query_string = query_template.substitute(task=taskUri)
    query_result = query_sudo(query_string)
    uris = []
    for res in query_result.get('results').get('bindings'):
        uris.append(res.get('graph').get('value'))
    return uris


def fetch_path_from_input_container(container):
    query_template= Template("""
        select ?path where  {
            $container <http://redpencil.data.gift/vocabularies/tasks/hasFile> ?file.
            ?path <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource> ?file.
        }
    """)
    query_string = query_template.substitute(container=sparql_escape_uri(container))
    query_result = query_sudo(query_string)
    if len(query_result.get('results').get('bindings')) >= 1:
        return query_result.get('results').get('bindings')[0].get('path').get('value')
    return None


def append_task_result_file(task, container_uri, container_id, file_uri):
   query_template =  Template("""
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
        PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
        PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
        INSERT DATA {
          GRAPH $task_graph {
            $container_uri a nfo:DataContainer.
            $container_uri mu:uuid $container_id.
            $container_uri task:hasFile $file_uri.

            $task_uri task:resultsContainer $container_uri.
          }
        }
        
    """)
   query_string = query_template.substitute(
           task_graph=sparql_escape_uri(task.get('graph','')),
           container_uri=sparql_escape_uri(container_uri),
           container_id=sparql_escape_string(container_id),
           file_uri=sparql_escape_uri(file_uri),
           task_uri=sparql_escape_uri(task.get('task',''))
           )
   update_sudo(query_string)

def write_ttl_file(graph, output_path, logical_file_name):
    phyId = generate_uuid()
    phyFilename = f"{phyId}.ttl"
    path = f"/share/{phyFilename}"
    os.rename(output_path, path)

    physicalFile = path.replace('/share/', 'share://')
    loId = generate_uuid()
    logicalFile = f"http://data.lblod.info/id/files/{loId}"
    now = sparql_escape_datetime(datetime.now())
    stats = os.stat(path)
    fileSize = stats.st_size
    query_template=Template("""
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX dbpedia: <http://dbpedia.org/ontology/>
      INSERT DATA {
        GRAPH $graph {
          $physicalFile a nfo:FileDataObject;
                                  nie:dataSource $logicalFile ;
                                  mu:uuid $phyId;
                                  nfo:fileName $phyFilename ;
                                  dct:creator <http://lblod.data.gift/services/harvesting-import-service>;
                                  dct:created $now;
                                  dct:modified $now;
                                  dct:format "text/turtle";
                                  nfo:fileSize $fileSize;
                                  dbpedia:fileExtension "ttl".
          $logicalFile a nfo:FileDataObject;
                                  mu:uuid $loId;
                                  nfo:fileName $logicalFileName ;
                                  dct:creator <http://lblod.data.gift/services/harvesting-import-service>;
                                  dct:created $now;
                                  dct:modified $now;
                                  dct:format "text/turtle";
                                  nfo:fileSize $fileSize;
                                  dbpedia:fileExtension "ttl" .
        }
      }""")
    query_string = query_template.substitute(
             graph= sparql_escape_uri(graph),
             physicalFile= sparql_escape_uri(physicalFile),
             logicalFile= sparql_escape_uri(logicalFile),
             phyId=sparql_escape_string(phyId),
             loId=sparql_escape_string(loId),
             phyFilename=sparql_escape_string(phyFilename),
             logicalFileName=sparql_escape_string(logical_file_name),
             now=now,
             fileSize=sparql_escape_int(fileSize))
    update_sudo(query_string)
    return logicalFile

def append_task_result_graph(task, container_uri,container_id, graph_uri):
    query_template = Template("""
                    PREFIX dct: <http://purl.org/dc/terms/>
                    PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
                    PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
                    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
                    INSERT DATA {
                      GRAPH $task_graph {
                        $container_uri a nfo:DataContainer.
                        $container_uri mu:uuid $container_id.
                        $container_uri task:hasGraph $graph_uri.

                        $task_uri task:resultsContainer $container_uri.
                      }
                    }
    """)
    query_string = query_template.substitute(
           task_graph=sparql_escape_uri(task.get('graph','')),
           container_uri=sparql_escape_uri(container_uri),
           container_id=sparql_escape_string(container_id),
           graph_uri=sparql_escape_uri(graph_uri),
           task_uri=sparql_escape_uri(task.get('task',''))
           )
    update_sudo(query_string)
