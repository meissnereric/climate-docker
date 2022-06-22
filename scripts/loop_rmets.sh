#!/bin/bash
models=("EC-Earth3")
#models=("EC-Earth3" "GFDL_ESM4")
#locations=("Dhaka" "Chicago")
locations=("Dhaka")

for model_name in ${models[@]}; do
    for location in ${locations[@]}; do
        model="s3://climate-ensembling/models/$model_name/"
        echo "$model $model_name $location"
        docker run -e AWS_ACCESS_KEY_ID=AKIAUGNY6UWO4MAMO3OD -e AWS_SECRET_ACCESS_KEY=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 climate /scripts/default.sh "cca" $model $model_name $location &>> $model_name-$location.log
        docker container prune -f
# ./default.sh "cca" $model $model_name $location

    done
done

