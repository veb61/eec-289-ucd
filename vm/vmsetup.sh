#!/bin/bash
if [[ $(lsb_release -rs) == "20.04" ]]; then
	if ! [ $(id -u) = 0 ]; then
	   echo "The script need to be run as root." >&2
	   exit 1
	fi
	
	if [ "$SUDO_USER" ]; then
	    real_user=$SUDO_USER
	else
	    real_user=$(whoami)
	fi
	
	apt-get update
	
	
     	apt-get install -y --no-install-recommends \
        apt-utils \
        nano \
        vim \
        curl \
        sudo \
        debhelper \
        build-essential \
        python3-setuptools \
        python3-all \
        python3-pip \
        wget \
        zlib1g-dev \
        libz3-dev \
        ninja-build \
        git \
        unzip \
        cmake \
        gdb \
        jq \
        valgrind \
        gcovr \
      	debian-keyring  \
      	debian-archive-keyring   \
      	apt-transport-https \
      	gnupg \
      	ca-certificates \
      	libc6-dev \
        linux-tools-common \
        linux-tools-aws \
        fakeroot
        
        curl -1sLf \
  	'https://dl.cloudsmith.io/public/eec289/eec289-f1/setup.deb.sh' \
  	| bash
  
	apt-get install opencilk=1.0

	rm -rf /var/lib/apt/lists
	
	pip3 install wheel
	
	pip3 install boto3
	
	pip3 install pyboto
	
	pip3 install stdeb==0.9.1
	
	pip3 install opentuner==0.8.3
else
       echo "Non-compatible version"
fi

