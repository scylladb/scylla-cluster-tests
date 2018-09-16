FROM centos:7

ADD install-prereqs.sh install-prereqs.sh
RUN ./install-prereqs.sh docker
ADD requirements-python.txt requirements-python.txt
RUN grep -v '^#' requirements-python.txt | xargs -t -L 1 pip install
CMD service docker start