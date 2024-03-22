FROM cassandra:3.11

RUN apt update && apt install python3 python3-pip vim awscli -y
RUN ln -s /usr/bin/python3 /usr/bin/python
