# base docker image
FROM python:3.12

WORKDIR /home/ubuntu/
EXPOSE 8501

# installation needed for client connection for db2
RUN apt-get update && \
    apt-get clean

# pip install all other libraries
COPY requirements.txt /home/ubuntu/
RUN pip install -r requirements.txt

COPY . /home/ubuntu/
 
# run script for service now table population
CMD ["bash"] 