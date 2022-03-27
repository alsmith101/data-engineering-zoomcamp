FROM python:3.9
# the base image to base our new image on

RUN pip install pandas
# The RUN instruction will execute any commands in a new layer on top of the current image and commit the results.
# The resulting committed image will be used for the next step in the Dockerfile

WORKDIR /app
# This creates a directory and cd's into it

COPY pipeline.py .
ENTRYPOINT ["python", "pipeline.py"]

# to build the image from the Dockerfile: docker build -t test:pandas file_path
# to run the new image: docker run -it test:pandas arg1=v1 arg2=v2