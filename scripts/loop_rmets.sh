#!/bin/bash
models=("EC-Earth-3" "GFDL_ESM4")
locations=("Dhaka" "Chicago")

for model_name in ${models[@]}; do
    for location in ${locations[@]}; do
        model="s3://climate-ensembling/models/$model_name/"
        echo "$model $model_name $location"
        ./default.sh "cca" $model $model_name $location
    done
done

