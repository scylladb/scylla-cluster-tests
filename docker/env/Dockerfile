FROM centos:7

ADD install-prereqs.sh install-prereqs.sh
RUN ./install-prereqs.sh docker
ADD requirements-python.txt requirements-python.txt
RUN pip install setuptools==40.8.0 pip==19.0.2
RUN grep -v '^#' requirements-python.txt | xargs -t -L 1 pip install
