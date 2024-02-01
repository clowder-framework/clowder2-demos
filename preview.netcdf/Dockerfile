FROM --platform=linux/amd64 ubuntu:18.04
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"
RUN apt-get update

RUN apt-get install -y wget && rm -rf /var/lib/apt/lists/*

RUN wget \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh
RUN conda --version

RUN echo $CONDA_PREFIX

COPY extractor_info.json .
COPY environment.yml .

COPY netcdfutils.py ncsa.geo.netcdf.extractor.py extractor_info.json ./

RUN conda install chardet

RUN conda install -c conda-forge mamba

RUN mamba env create -f environment.yml

SHELL ["conda", "run", "-n", "netcdf-preview", "/bin/bash", "-c"]

RUN python -m pip install pyclowder

ENV CLOWDER_VERSION=2

CMD ["conda", "run", "--no-capture-output", "-n", "netcdf-preview", "python","-u", "/ncsa.geo.netcdf.extractor.py"]