FROM python:3.7

COPY ./requirements/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY ./scripts/ /scripts
RUN chmod 755 /scripts/*

COPY ./app /app
RUN ls /app

RUN chmod 755 /app/*

CMD /scripts/default.sh "pd"
