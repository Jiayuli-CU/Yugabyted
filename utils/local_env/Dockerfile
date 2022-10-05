FROM ubuntu

# install required dependncies
RUN apt-get update
RUN apt-get -y install wget
RUN apt install git --yes

# install yugabytedb
RUN wget https://downloads.yugabyte.com/releases/2.15.2.0/yugabyte-2.15.2.0-b87-linux-x86_64.tar.gz
RUN tar xvfz yugabyte-2.15.2.0-b87-linux-x86_64.tar.gz 
RUN ./yugabyte-2.15.2.0/bin/post_install.sh

# install golang
RUN wget https://go.dev/dl/go1.19.1.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.19.1.linux-amd64.tar.gz
RUN echo "export PATH=\$PATH:/usr/local/go/bin:\$HOME/go/bin" >> ~/.bashrc
RUN echo "export GOROOT=/usr/local/go" >> ~/.bashrc 
RUN . ~/.bashrc 

# remove install packages
RUN rm -r yugabyte-2.15.2.0-b87-linux-x86_64.tar.gz && cd yugabyte-2.15.2.0/
RUN rm -r go1.19.1.linux-amd64.tar.gz