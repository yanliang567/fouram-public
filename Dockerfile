FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    software-properties-common \
    curl \
    build-essential \
    libssl-dev \
    libffi-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    liblzma-dev \
    && rm -rf /var/lib/apt/lists/*

# add deadsnakes PPA for multiple Python
RUN add-apt-repository ppa:deadsnakes/ppa

RUN apt-get update && apt-get install -y \
    python3.8 \
    python3.8-dev \
    python3.8-venv \
    python3.12 \
    python3.12-dev \
    python3.12-venv \
    && rm -rf /var/lib/apt/lists/*

RUN curl -sS https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3.8 get-pip.py \
    && python3.12 get-pip.py \
    && rm get-pip.py

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 2

RUN apt-get -y update && apt-get -y install sudo git vim wget nfs-common mysql-client jq tree --fix-missing \
    && sudo apt-get install -y python3.8-dbg python3.12-dbg \
    && python3.8 -m pip install -U pip setuptools \
    && python3.12 -m pip install -U pip setuptools

RUN wget https://go.dev/dl/go1.22.0.linux-amd64.tar.gz && tar -C /usr/local -xzf go1.22.0.linux-amd64.tar.gz
ENV PATH $PATH:/usr/local/go/bin

RUN wget --no-check-certificate -qO- "https://get.helm.sh/helm-v3.16.3-linux-amd64.tar.gz" | tar --strip-components=1 -xz -C /usr/local/bin linux-amd64/helm
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl \
    && chmod +x kubectl && mv kubectl /usr/local/bin/


COPY Dockerfile /Dockerfile
COPY dependencies/requirements_3.8.txt /requirements_3.8.txt
COPY dependencies/requirements_3.12.txt /requirements_3.12.txt

RUN sudo -H python3.8 -m pip install --ignore-installed -U blinker && python3.8 -m pip install --no-cache-dir -r /requirements_3.8.txt
RUN sudo -H python3.12 -m pip install --ignore-installed -U blinker && python3.12 -m pip install --no-cache-dir -r /requirements_3.12.txt

WORKDIR /root
