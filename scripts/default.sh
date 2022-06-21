#!/bin/bash

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
                            "standardised_calendar": "s3://climate-ensembling/calendar/era5/",

                            "location": "'"$LOCATION"'",
                            "start": "1980-01-01",
                            "end": "2020-01-01",

                            "reference":  "s3://climate-ensembling/reference/era5/",
                            "past": ["1980-01-01", "2000-01-01"],
                            "future": ["2000-01-01", "2020-01-01"],
                            "bias_correction_method": "none",

                            "reference": "s3://climate-ensembling/reference/era5/",
                            "window": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100],
                            "threshold": "300",
                            "threshold_type": "lower"
                        }
                    '}
SELECT_LOCATION={'"dataId": "SelectLocation--5846990926543449174",
                                "service_name": "SelectLocation",
                                "inputs": {"ProcessData": "s3://climate-ensembling/tst/outputs/ProcessData/"},
                                "outputs": {"Output1" : "s3://climate-ensembling/tst/outputs/SelectLocation/"},
                                "parameters": {"SelectLocation": {
                                "location": "Dhaka",
                                "start": "1980-01-01",
                                "end": "2050-01-01"}
                                }
                            '}

PROCESS_DATA={'"dataId": "ProcessData--5846990926543449174",
                                "service_name": "ProcessData",
                                "inputs": {},
                                "outputs": {"Output1" : "s3://climate-ensembling/tst/outputs/ProcessData/"},
                                "parameters": {"ProcessData": {
                                "model": "s3://climate-ensembling/tst/EC-Earth3/",
                                "standardised_calendar": "s3://climate-ensembling/tst/EC-Earth3/"}
                                }
                            '}

TEST_SELECTION=$1

if [ "$TEST_SELECTION" = "pd" ]; then
    PARAMETERS=$PROCESS_DATA
elif [ "$TEST_SELECTION" = "sl" ]; then
    PARAMETERS=$SELECT_LOCATION
elif [ "$TEST_SELECTION" = "cca" ]; then
    PARAMETERS=$CALCULATE_COSTS_ALL
else
    PARAMETERS=$1
fi

echo "... Parameters: $PARAMETERS"
# ../app/main.py --parameters="$PARAMETERS"