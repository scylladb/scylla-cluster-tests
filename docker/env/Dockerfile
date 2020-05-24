FROM centos:7

ADD install-prereqs.sh install-prereqs.sh
RUN ./install-prereqs.sh docker
RUN yum update -y \
    && yum install -y https://repo.ius.io/ius-release-el7.rpm \
    && yum install -y python36u python36u-libs python36u-devel python36u-pip

ADD requirements-python.txt requirements-python.txt
RUN pip3 install setuptools==40.8.0 pip==19.1.1
RUN pip3 install -r requirements-python.txt

ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV PYTHONWARNINGS ignore:unclosed
