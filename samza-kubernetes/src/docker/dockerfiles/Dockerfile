#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


FROM ubuntu:latest

# TODO: build samza base image. This is used to build the image of hello-samza example.
# Before building the docker image, first build and make a Samza distribution following
# the instructions in https://github.com/apache/samza.
# If this docker file is being used in the context of building your images from a Samza distribution,
# the docker build command should be invoked from the top level directory
# of the Samza distribution. E.g.:
# docker build -t samza:latest -f kubernetes/dockerfiles/Dockerfile .

RUN apt-get update -y
RUN apt-get upgrade -y

# install openjdk
RUN apt-get install -y openjdk-8-jdk
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin

# install and configure ssh service
RUN apt-get install -y openssh-server \
    && mkdir -p /var/run/sshd

# install general tools
RUN apt-get install -y iproute2 vim inetutils-ping

RUN apt-get clean

RUN mkdir -p /opt/hello-samza
WORKDIR /opt/hello-samza/
COPY deploy/samza /opt/hello-samza/
