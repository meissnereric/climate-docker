FROM python:3.7

COPY ./requirements/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt


COPY ./app /app
RUN ls /app

RUN chmod 755 /app/*

CMD /app/main.py --parameters={'"dataId": "SelectLocation--5846990926543449174", \
                                "service_name": "SelectLocation", \
                                "inputs": {}, \
                                "outputs": {"Output1" : "s3://climate-ensembling/tst/outputs/"}, \
                                "parameters": {"SelectLocation": {\
                                "model": "s3://climate-ensembling/tst/EC-Earth3/",\
                                "location": "Dhaka",\
                                "start": "1980-01-01",\
                                "end": "2050-01-01"}\
                                }\
                            '}



