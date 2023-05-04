export $(cat .env | xargs)

source spotify_project_venv/bin/activate
prefect deployment build -n "Spotify Top Charts Flow" \
    -ib cloud-run-job/spotify-cloud-run-job \
    flows/etl_flow.py:etl_flow \
    -q default -a --path /app/flows
prefect deployment run "Main flow/etl-flow"
