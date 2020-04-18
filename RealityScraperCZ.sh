#!/bin/bash

source $HOME/github/RealityScraperCZ_env/bin/activate
PATH=$PATH:/usr/local/bin
export PATH
cd $HOME/github/RealityScraperCZ
scrapy crawl sreality
