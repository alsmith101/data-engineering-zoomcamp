FROM python:3.9
# the base image to base our new image on

RUN pip install pandas SQLAlchemy psycopg2
# The RUN instruction will execute any commands in a new layer on top of the current image and commit the results.
# The resulting committed image will be used for the next step in the Dockerfile

RUN apt-get install wget

WORKDIR /app
# This creates a directory and cd's into it

COPY ingest_data.py .
ENTRYPOINT ["python", "ingest_data.py"]

# to build the image from the Dockerfile: docker build -t test:pandas file_path
# to run the new image: docker run -it test:pandas arg1=v1 arg2=v2