# Climate Ensembling Docker Setup

## Build and upload the new container

You can run the `build_and_upload.sh` script to re-build and load the container to the ECR repository so long as you've configured your AWS credentials through `aws configure`.

The below text explains what it's looking at.

### Running the image locally

Build it
```
docker build -t climate .
```

Run it
```
docker run -e AWS_ACCESS_KEY_ID=<KEY> -e AWS_SECRET_ACCESS_KEY=<KEY> climate
```

### Pushing the Docker Image to ECR
(Referenced from [here](https://docs.aws.amazon.com/AmazonECR/latest/userguide/docker-push-ecr-image.html).)

Login to ECR on your console.
```
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 288687564189.dkr.ecr.eu-west-1.amazonaws.com/climate-ensembling
```

Associate the climate container locally with the ECR repository
```
docker tag climate 288687564189.dkr.ecr.eu-west-1.amazonaws.com/climate-ensembling:climate
```

Push to ECS
```
docker push 288687564189.dkr.ecr.eu-west-1.amazonaws.com/climate-ensembling:climate
```
