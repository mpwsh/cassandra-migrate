FROM centos:centos7
RUN yum install -y curl wget unzip zip ca-certificates java-1.8.0-openjdk-devel tmux
RUN yum groupinstall "Development Tools" -y
COPY ./apache-cassandra-tools.tar.gz /opt
WORKDIR /opt
RUN tar xvfz apache-cassandra-tools.tar.gz
ENV PATH="/opt/apache-cassandra-3.11.4/bin:${PATH}"
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
RUN wget http://python.org/ftp/python/2.7.6/Python-2.7.6.tgz
RUN tar -xvzf Python-2.7.6.tgz
WORKDIR Python-2.7.6
RUN ./configure --prefix=/usr/local
RUN make
RUN make install
WORKDIR /root
