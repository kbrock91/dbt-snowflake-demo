{% macro create_external_api_udf() %}
    
CREATE OR REPLACE NETWORK RULE official_joke_api_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('official-joke-api.appspot.com');

  CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION joke_apis_access_integration
  ALLOWED_NETWORK_RULES = (official_joke_api_network_rule)
  ENABLED = true;

  CREATE OR REPLACE FUNCTION joke_python()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'get_joke'
EXTERNAL_ACCESS_INTEGRATIONS = (joke_apis_access_integration)
PACKAGES = ('snowflake-snowpark-python','requests')
AS
$$
import _snowflake
import requests
import json
session = requests.Session()
def get_joke():
  url = "https://official-joke-api.appspot.com/random_joke"
  response = session.get(url)
  return response.json()
$$;

select joke_python()

{% endmacro %}