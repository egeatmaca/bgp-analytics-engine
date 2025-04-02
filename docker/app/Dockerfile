FROM python:3.11-bullseye

# Install dependencies
RUN apt-get update &&\
    apt-get install -y curl apt-transport-https ssl-cert ca-certificates gnupg lsb-release

# Install Libwandio repository
RUN curl -1sLf 'https://dl.cloudsmith.io/public/wand/libwandio/cfg/setup/bash.deb.sh' | bash

# Install CAIDA repository
RUN echo "deb https://pkg.caida.org/os/$(lsb_release -si|awk '{print tolower($0)}') $(lsb_release -sc) main" | tee /etc/apt/sources.list.d/caida.list &&\
    wget -O /etc/apt/trusted.gpg.d/caida.gpg https://pkg.caida.org/os/ubuntu/keyring.gpg 
   
# Install BGPStream
RUN apt-get update &&\
    apt-get install -y bgpstream

# Set working directory
WORKDIR /app

# Copy files for build
COPY . .

# Install Python requirements 
RUN pip install -r requirements.txt

# Set entrypoint
ENTRYPOINT ["/bin/bash", "entrypoint.sh"]




