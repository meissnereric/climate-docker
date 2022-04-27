#!/bin/bash

aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 288687564189.dkr.ecr.eu-west-1.amazonaws.com/climate-ensembling

docker build -t climate .

docker tag climate 288687564189.dkr.ecr.eu-west-1.amazonaws.com/climate-ensembling:climate

 docker push 288687564189.dkr.ecr.eu-west-1.amazonaws.com/climate-ensembling:climate