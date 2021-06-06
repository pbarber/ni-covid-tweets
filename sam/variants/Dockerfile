FROM public.ecr.aws/lambda/python:3.8

RUN yum -y install amazon-linux-extras
RUN PYTHON=python2 amazon-linux-extras enable epel -y
RUN yum -y install epel-release wget unzip
RUN yum -y install chromium
RUN wget -P /tmp/ https://chromedriver.storage.googleapis.com/90.0.4430.24/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver_linux64.zip -d /tmp/
RUN mv /tmp/chromedriver /usr/bin/chromedriver

COPY variants/requirements.txt .
RUN python3.8 -m pip install -r requirements.txt

COPY shared/__init__.py shared/
COPY variants/app.py .

# Overwrite the command by providing a different command directly in the template.
CMD ["app.lambda_handler"]