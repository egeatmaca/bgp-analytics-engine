FROM python:3.11-bullseye

# Install dependencies
RUN apt-get update &&\
    apt-get install -y --no-install-recommends openjdk-17-jdk curl apt-transport-https ssl-cert ca-certificates gnupg lsb-release
    
# Install Spark
ENV SPARK_HOME="/opt/spark"
ENV SPARK_MASTER_PORT="7077"
ENV SPARK_MASTER_HOST="spark-master"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="${JAVA_HOME}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

RUN mkdir -p ${SPARK_HOME}

WORKDIR ${SPARK_HOME}

RUN curl https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz -o spark-3.5.5-bin-hadoop3.tgz \
    && tar xvzf spark-3.5.5-bin-hadoop3.tgz --directory ${SPARK_HOME} --strip-components 1 \
    && rm -rf spark-3.5.5-bin-hadoop3.tgz

# Install Libwandio repository
RUN curl -1sLf 'https://dl.cloudsmith.io/public/wand/libwandio/cfg/setup/bash.deb.sh' | bash

# Install CAIDA repository
RUN echo "deb https://pkg.caida.org/os/$(lsb_release -si|awk '{print tolower($0)}') $(lsb_release -sc) main" | tee /etc/apt/sources.list.d/caida.list &&\
    wget -O /etc/apt/trusted.gpg.d/caida.gpg https://pkg.caida.org/os/ubuntu/keyring.gpg 
   
# Install BGPStream
RUN apt-get update &&\
    apt-get install -y bgpstream

# Clean apt build cache
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy files for build
COPY . .

# Install Python requirements 
RUN pip install -r requirements.txt

# Set entrypoint
ENTRYPOINT ["/bin/bash", "entrypoint.sh"]




