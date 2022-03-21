FROM python:3.7

COPY ./requirements/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt


COPY ./app /app
RUN ls /app

RUN chmod 755 /app/*

CMD /app/main.py --parameters={"dataId": "SelectLocation--5846990926543449174","service_name": "SelectLocation","inputs": {},"outputs": {},"parameters": {"SelectLocation": {"models": ["s3://climate-ensembling/EC-Earth3/","s3://climate-ensembling/EC-Earth3/"],"locations": ["Dhaka"]}}}



