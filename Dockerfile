FROM python:3.7-slim-buster as build
LABEL   app.authors="Arturo C. <acampos@attachmedia.com>" \
        app.name="Marketing attribution for Scotiabank Peru" \
        app.description="Applying attribution models on Google Analytics 4 data for web and app properties."

WORKDIR /app

ADD ./requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

ADD ./install_security_updates.sh /app/install_security_updates.sh

RUN sh install_security_updates.sh

COPY . /app/

RUN . ./env.sh && \
    python3 app.py
    