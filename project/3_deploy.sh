export $(cat .env | xargs)

source spotify_project_venv/bin/activate
prefect deployment build flows/etl_flow.py:etl_flow -n etl-flow --apply
prefect deployment apply etl_flow-deployment.yaml
prefect deployment run "Main flow/etl-flow"
