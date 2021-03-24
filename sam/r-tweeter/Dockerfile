FROM public.ecr.aws/lambda/python:3.8

COPY r-tweeter/requirements.txt ./
RUN python3.8 -m pip install -r requirements.txt

RUN mkdir -p ./shared
COPY shared/__init__.py ./shared/__init__.py
COPY r-tweeter/app.py ./

# Overwrite the command by providing a different command directly in the template.
CMD ["app.lambda_handler"]