#!/bin/bash

source $HOME/github/RealityScraperCZ_env/bin/activate
PATH=$PATH:/usr/local/bin
export PATH
cd $HOME/github/RealityScraperCZ
docker start "80a9b83ac2aec74c8aacec9d29dc646cbf5b6d0dc5655ee7e1cece9223a6bba1"
scrapy crawl sreality
