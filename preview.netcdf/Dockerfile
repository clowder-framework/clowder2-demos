FROM python:3.8

WORKDIR /extractor
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY netcdfutils.py ncsa.geo.netcdf.extractor.py extractor_info.json ./
ENV CLOWDER_VERSION=2.0
CMD python ncsa.geo.netcdf.extractor.py