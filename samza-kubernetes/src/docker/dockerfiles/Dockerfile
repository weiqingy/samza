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

# may need to update the following jars during debugging
COPY samza-core_2.11-1.0.1-SNAPSHOT.jar /opt/hello-samza/lib/
COPY samza-kubernetes_2.11-1.0.1-SNAPSHOT.jar  /opt/hello-samza/lib/
COPY run-jc.sh /opt/hello-samba/bin/



