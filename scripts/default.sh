#!/bin/bash

TYPE=$1
MODEL=$2
MODEL_NAME=$3
LOCATION=$4
OUTPUT="s3://climate-ensembling/rmets/model-$MODEL_NAME/location-$LOCATION/"

CALCULATE_COSTS_ALL={'
                        "dataId": "CalculateCostsAll--5846990926543449174",
                        "service_name": "CalculateCostsAll",
                        "inputs": {},
                        "outputs": {"Output1" : "'"$OUTPUT"'"},
                        "parameters": {
                            "model": "'"$MODEL"'",
                            "standardised_calendar": "s3://climate-ensembling/reference/era5/tas/",

                            "location": "'"$LOCATION"'",
                            "start": "1979-01-01",
                            "end": "1981-12-30",

                            "reference":  "s3://climate-ensembling/reference/era5/tas/",
                            "past": ["1979-01-01", "1981-01-01"],
                            "future": ["1981-01-01", "1981-12-30"],
                            "bias_correction_method": "none",

                            "window": [3,5],
                            "threshold": "300",
                            "threshold_type": "lower"
                        }
                    '}

TEST_SELECTION=$TYPE

if [ "$TEST_SELECTION" = "pd" ]; then
    PARAMETERS=$PROCESS_DATA
elif [ "$TEST_SELECTION" = "sl" ]; then
    PARAMETERS=$SELECT_LOCATION
elif [ "$TEST_SELECTION" = "cca" ]; then
    PARAMETERS=$CALCULATE_COSTS_ALL
else
    PARAMETERS=$TYPE
fi

PARAMETERS=$CALCULATE_COSTS_ALL

echo "$1 $2 $3 $4"

echo "... Parameters: $PARAMETERS"
 ../app/main.py --parameters="$PARAMETERS"
