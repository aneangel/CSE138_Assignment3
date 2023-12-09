# I used Cheng-Wei Dockerfile that he provided for the class to help wrtie this file
# found here: https://canvas.ucsc.edu/courses/64879/assignments/518264 as well as, Docker resources
# found here: https://www.docker.com/blog/how-to-dockerize-your-python-applications/


FROM python:3

RUN pip install flask
RUN pip install requests

COPY lib ./lib
COPY server.py ./

CMD [ "python3", "./server.py" ]