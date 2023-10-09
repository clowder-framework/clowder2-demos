ARG PYCLOWDER_PYTHON=""
FROM clowder/pyclowder${PYCLOWDER_PYTHON}:onbuild

ENV MAIN_SCRIPT="concatenator.py" \
    RABBITMQ_QUEUE=""

ONBUILD COPY extractor_info.json /home/clowder/
