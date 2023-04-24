export $(cat .env | xargs)

# install dependencies
sudo apt-get install python3-venv
python3 -m venv spotify_project_venv
source spotify_project_venv/bin/activate
pip install -r requirements.txt

# start prefect agent
prefect cloud login -k pnu_iKNmGcmslZs8wbN5xHXYwoRLAQPOF14GlPT3
prefect agent start -q 'default'