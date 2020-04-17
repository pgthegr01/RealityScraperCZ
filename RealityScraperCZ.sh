#!/bin/bash

source $HOME/github/RealityScraperCZ_env/bin/activate
PATH=$PATH:/usr/local/bin
export PATH
scrapy crawl sreality
