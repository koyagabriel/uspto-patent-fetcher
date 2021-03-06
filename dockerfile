FROM python:latest
LABEL maintainer="koyagabriel@gmail.com"
WORKDIR /var/www
COPY requirements.txt /var/www
RUN pip install -r requirements.txt
COPY . /var/www
ENTRYPOINT ["python", "main.py"]