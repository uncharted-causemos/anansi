# Builds a dockerized webservice
FROM mhart/alpine-node:16

# Python stuff
COPY requirements-web.txt requirements.txt

RUN apk update 
RUN apk add curl 
RUN apk add gcc
RUN apk add --no-cache python3 py3-pip

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY ./src ./ 

WORKDIR web
RUN chmod 777 web/dart.sh
RUN npm install

CMD ["node", "index.js"]
