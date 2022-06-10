#!/bin/bash

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
else
    PARAMETERS=$1
fi


/app/main.py --parameters="$PARAMETERS"