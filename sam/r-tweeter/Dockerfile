FROM public.ecr.aws/lambda/python:3.8

RUN yum -y install amazon-linux-extras
RUN PYTHON=python2 amazon-linux-extras enable epel -y
RUN yum -y install epel-release wget unzip
RUN yum -y install chromium-99.0.4844.84
RUN wget -P /tmp/ https://chromedriver.storage.googleapis.com/99.0.4844.51/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver_linux64.zip -d /tmp/
RUN mv /tmp/chromedriver /usr/bin/chromedriver
RUN yum -y install java-1.8.0-openjdk

COPY r-tweeter/requirements.txt ./
RUN python3.8 -m pip install -r requirements.txt

COPY shared/__init__.py ./shared/__init__.py
COPY twitter_shared/__init__.py ./twitter_shared/__init__.py
COPY plot_shared/__init__.py plot_shared/
COPY data_shared/__init__.py data_shared/
COPY nisra-tweeter/app.py ./nisra_tweeter.py
COPY ons-tweeter/app.py ./ons_tweeter.py

# Overwrite the command by providing a different command directly in the template.
CMD ["app.lambda_handler"]