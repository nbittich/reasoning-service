TASK_HARVESTING_REASONING = 'http://lblod.data.gift/id/jobs/concept/TaskOperation/reasoning'

STATUS_BUSY = 'http://redpencil.data.gift/id/concept/JobStatus/busy'
STATUS_SCHEDULED = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled'
STATUS_SUCCESS = 'http://redpencil.data.gift/id/concept/JobStatus/success'
STATUS_FAILED = 'http://redpencil.data.gift/id/concept/JobStatus/failed'

JOB_TYPE = 'http://vocab.deri.ie/cogs#Job'
TASK_TYPE = 'http://redpencil.data.gift/vocabularies/tasks/Task'
ERROR_TYPE= 'http://open-services.net/ns/core#Error'
ERROR_URI_PREFIX = 'http://redpencil.data.gift/id/jobs/error/'

PREFIXES = """
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
"""
