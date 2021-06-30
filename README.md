# NI COVID-19 data bot

This repository holds the source code for [an unofficial Twitter bot](https://twitter.com/ni_covid19_data) which summarises and posts data about COVID-19 in Northern Ireland (NI). The bot posts:

* daily updates on vaccinations
* daily updates on tests and cases
* weekly updates on R number estimates
* weekly updates on NISRA deaths statistics
* ad-hoc updates of COG-UK variant counts
* occasional interesting charts

## Vaccinations

Vaccinations data is taken from either [HSCNI](https://covid-19.hscni.net/) or [PHE](https://coronavirus.data.gov.uk/), whichever posts first (generally HSCNI but both processes seem to be manual so there is some variability).

HSCNI data is scraped from the front page of the website, PHE data is taken from the [API](https://coronavirus.data.gov.uk/details/download). PHE API fields used are:

* cumPeopleVaccinatedFirstDoseByPublishDate
* cumPeopleVaccinatedSecondDoseByPublishDate
* cumVaccinationFirstDoseUptakeByPublishDatePercentage
* cumVaccinationSecondDoseUptakeByPublishDatePercentage

## Tests and cases

Tests and cases (aka positive tests) data is taken from the daily Excel [downloads](https://www.health-ni.gov.uk/articles/covid-19-daily-dashboard-updates) of the NI Department of Health (DoH) dashboard. The information presented is taken from the `Summary Tests`, `Deaths`, `Admissions` and `Discharges` tabs.

The bot runs an exponential curve fitting model, using a 9-day window over the 7-day case average, to calculate the current growth rate of cases.

Note that the DoH daily data is, in my opinion, somewhat flawed. The dashboard and download provide a count `All Individuals Tested` for each day, this is not all individuals tested on that day, but the total of individuals who were last tested on that day. So, if I have been tested twice, once in September 2020 and once in March 2021, then I was counted in the September figures up until March, when my count was moved to the March numbers.

In addition, the daily tests reported can be considered incomplete for a number of days after the initial report. As in many parts of the world, NI data on negative tests moves slower than data on positive tests. There's a [blog post](https://codeandnumbers.co.uk/data-dynamics-and-covid-19/) on this topic.

## R number

The DoH publishes [R number estimates](https://www.health-ni.gov.uk/R-Number) once a week. The headlines of this report relative to R estimates are presented as is.

## Deaths

NISRA publishes [weekly deaths statistics](https://www.nisra.gov.uk/publications/weekly-death-statistics-northern-ireland-2021) when the number of deaths registered in a week is five or more.

## Variants

Variant data is pulled from [Microreact](https://beta.microreact.org/) as new files are published to its AWS S3 website.

## Architecture

The bot is built using AWS infrastructure. The architecture is:

* one 'scraper' for all data sources, running as a Python lambda function scheduled via EventBridge
* five 'tweeters' for the five types of post, running as Python lambda functions triggered by the scraper when the data changes
    * the more complicated (with charts or PDF extraction) of these use Dockerfiles, others use S3/lambda zip files
* an S3 bucket to hold the scraped data
* an S3 bucket to hold the lambda function code
* an ECR repository to hold the images for the more complex tweeters
* a Secrets Manager secret which holds API keys and other parameters

### User setup

A new AWS user group was created with CLI access and the following AWS policies.

* `ReadOnlyAccess`
* `AmazonS3FullAccess`
* `IAMFullAccess`
* `SecretsManagerReadWrite`
* `AWSLambda_FullAccess`
* `CloudWatchEventsFullAccess`
* `AmazonEC2ContainerRegistryFullAccess`

### Infrastructure setup

Day-to-day management of the infrastructure is done through AWS [SAM CLI](https://aws.amazon.com/serverless/sam/) but the initial basic setup was via the [AWS CLI](https://aws.amazon.com/cli/). Most (if not all) of the setup could be achieved via SAM.

Things you will need to know:

* `PROFILE`: the AWS command line profile to use
* `BUCKETNAME`: the name of the bucket where you will store the scraped data
* `REGION`: the AWS region where the infrastructure will be created

Create s3 bucket to hold data:

```bash
aws --profile <PROFILE> s3api create-bucket --bucket <BUCKETNAME> --region <REGION> --create-bucket-configuration LocationConstraint=<REGION>
```

Create lambda S3 access policy, which will return `POLICYARN`:

```bash
aws --profile <PROFILE> iam create-policy --policy-name ni-covid-tweets-lambda --policy-document file://ni-covid-tweets-lambda-policy.json
```

Create lambda role with trust policy to allow lambda to assume the role:

```bash
aws --profile <PROFILE> iam create-role --role-name ni-covid-tweets --assume-role-policy-document file://ni-covid-tweets-lambda-trust-policy.json
```

Add the S3 access policy to the role:

```bash
aws --profile <PROFILE> iam attach-role-policy --role-name ni-covid-tweets --policy-arn <POLICYARN>
```

Create the secret:

```bash
aws --profile <PROFILE> secretsmanager create-secret --name ni-covid-tweets --secret-string `cat secrets.json`
```

Allow access to the secret (update the existing policy):

```bash
aws --profile <PROFILE> iam create-policy-version --policy-arn <POLICYARN> --policy-document file://../ni-covid-tweets-lambda-policy.json --set-as-default
```

Create an ECR repository which will return `ECRREPOID`:

```bash
aws --profile <PROFILE> ecr create-repository --repository-name ni-covid-tweets --image-tag-mutability IMMUTABLE --image-scanning-configuration scanOnPush=true
```

### SAM

The [requirements file](requirements.txt) in the repository root can be used to set up a [Python virtual environment](https://docs.python.org/3/tutorial/venv.html) which include SAM.

To build and test locally:

```bash
sam build <function> && sam local invoke --profile <PROFILE> <function> --event events/new-<event>.json
```

To deploy:

```bash
sam build && sam deploy --profile <PROFILE> --image-repository <ECRREPOID>.dkr.ecr.<REGION>.amazonaws.com/ni-covid-tweets
```
