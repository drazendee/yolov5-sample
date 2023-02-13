FROM pytorch/pytorch:1.10.0-cuda11.3-cudnn8-runtime

RUN apt-get update 
RUN apt-get install ffmpeg libsm6 libxext6  -y

RUN apt-get install git -y

ADD requirements.txt .
RUN pip install -r requirements.txt

ADD yolov5-requirements.txt .
RUN pip install -r yolov5-requirements.txt