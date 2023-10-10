FROM python:3.8

ENV MAIN_SCRIPT="concatenator.py" \
    RABBITMQ_QUEUE=""

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY concatenator.py extractor_info.json ./
CMD python concatenator.py
