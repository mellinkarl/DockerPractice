FROM python:3.12
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip3 install -r requirements.txt
COPY . .
ENV PORT=8080
EXPOSE ${PORT}
ENV GOOGLE_APPLICATION_CREDENTIALS='./key.json'
ENV INSTANCE_CONNECTION_NAME='hw3-mellinka:us-central1:hw3-instance'
ENV DB_NAME='hw3-mellinka-db'
ENV DB_USER='hw3-user'
ENV DB_PASS='abcd'
CMD [ "python", "main.py" ]
