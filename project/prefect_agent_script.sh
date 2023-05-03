#!/bin/bash
sudo apt-get update -y
sudo apt-get upgrade -y
sudo apt-get install -y \
    wget \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    software-properties-common \
    python3-dateutil \
    python3-distutils \
    python3-apt \
    tmux
sudo ln -s /usr/bin/python3 /usr/bin/python
wget https://bootstrap.pypa.io/get-pip.py
sudo python3 get-pip.py
PATH="$HOME/.local/bin:$PATH"
export PATH
pip3 install prefect prefect-gcp
prefect cloud login -k <PREFECT_CLOUD_API_KEY>
tmux new-session -d -s prefect 'prefect agent start -q default’