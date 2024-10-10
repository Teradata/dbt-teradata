{#-
This is a staging model. 
All we do is:
- Point to incoming data (from an external storage or loaded by a tool),
- Add standard control columns for this layer (eg. when was the record received),
- If necesary, override physical data types (eg. if not properly preserved by the E/L tools).
#}

{#- 
In this example we are loading from a seed, 
we mock the record update time with a static value 
(in order toto illustrate delta capture downstream) 
-#}
locking row for access
select
source.*
{%- if  var('last_update_ts') %}
,'{{ run_started_at }}' (timestamp) {{var('last_update_ts')}}
{%- endif %}
from  {{ ref('raw_payments') }} source