FROM python:3.7

COPY ./requirements/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt


COPY ./app /app
RUN ls /app

RUN chmod 755 /app/*

CMD /app/main.py --task='select_location' --coordinates='Dhaka'



