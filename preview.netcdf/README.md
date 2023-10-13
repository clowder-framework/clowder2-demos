# Clowder Geo NetCDF Extractor

Overview

This extractor uses python NetCDF4 and matplotlib to plot data from 
.nc and .nc4 files on a map.

NOTE - this is supposed to be a general purpose extractor that should work on 
many files, but because NetCDF is a flexible file format, it is not guaranteed to work.
If the data is a time series, it will generate 4 previews spaced evenly throughout the time interval.


## Build a docker image
      docker build -t clowder/extractors-geo-netcdf .

## Test the docker container image:
      docker run --name=geotiff-metadata -d --restart=always -e 'RABBITMQ_URI=amqp://user1:pass1@rabbitmq.ncsa.illinois.edu:5672/clowder-dev' -e 'RABBITMQ_EXCHANGE=clowder' -e 'TZ=/usr/share/zoneinfo/US/Central' -e 'REGISTRATION_ENDPOINTS=http://dts-dev.ncsa.illinois.edu:9000/api/extractors?key=key1' clowder/extractors-geotiff-metadata

## To run without docker 


1. Setup a [virtualenv](https://virtualenv.pypa.io), e.g., named "geo-netcdf":

   `virtualenv geo-netcdf`
2. Activate the virtualenv

   `source geo-netcdf`/bin/activate`
3. Install required python packages using *pip*

   `pip install -r requirements.txt`
4. Install pyclowder if it is not installed yet.

   `pip install git+https://opensource.ncsa.illinois.edu/stash/scm/cats/pyclowder.git`

   or if you have pyclowder checked out as well (useful when developing)

   `ln -s ../../pyClowder/pyclowder pyclowder`
5. Modify config.py
6. Start extractor

   `./ncsa.geo.netcdf.py`
