# Climate Ensembling Docker Setup

## Pushing the Docker Image to ECR
Referenced from [here](https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html).
```
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 288687564189.dkr.ecr.eu-west-1.amazonaws.com/climate-ensembling
```

```
docker tag 1c5f8168cd29 288687564189.dkr.ecr.eu-west-1.amazonaws.com/climate-ensembling:hello-world
```

```
docker push 288687564189.dkr.ecr.eu-west-1.amazonaws.com/climate-ensembling:hello-world
```

### Parameters

* Shared container
* Bias correction method
* Which model to use
* Force re-run
* The task to run (location selection, bias correction, aggregation, etc.)


### S3 Storage
Work in progress proposal?

```climate-ensembling/<task_name>/<parameters>/<datetime>/parameters.json,DATAFILE```

## General Setup

### Client script v1
- Takes in parameters to run
- Calls the first task (location -> Dhaka for v1) on each model
- Listens / waits for those tasks to finish, and when they finish calls the subsequent task in their computational chain (i.e. apply bias correction for that model).
- Listens and waits for them all to finish, prints that it's done (eventually perform the third+ tasks of aggregation.)
 
### Container Work
- Takes in parameters
- Checks the appropriate S3 bucket, if the output of this task with these paramters has already been computed, simply return / finish / inform the caller that it's done.
- Else, run the relevant code, pulling the inputs in from S3 and then outputting to the right bucket so that the next time it will be cached.


