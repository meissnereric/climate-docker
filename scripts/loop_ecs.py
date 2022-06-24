#!/usr/bin/env python
import boto3
import time


CLUSTER='HypervisorCluster'

def monitor_tasks(client, models, locations):
    print("Monitoring tasks.....")
    total_tasks = len(models) * len(locations)
    done = False
    i=0
    start = time.time()
    mid = time.time()
    end = time.time()
    print(end - start)
    while not done:
        end = time.time()
        if end - mid > 10:
            mid = end
            running_response = client.list_tasks(
                cluster=CLUSTER,
                desiredStatus='RUNNING'
            )
            pending_response = client.list_tasks(
                cluster=CLUSTER,
                desiredStatus='PENDING'
            )
            running = running_response['taskArns']
            pending = pending_response['taskArns']
            waiting = running + pending
            if len(waiting) >  0:
                done=False
            else:
                done=True
                stopped_response = client.list_tasks(
                    cluster=CLUSTER,
                    desiredStatus='STOPPED'
                )
                stopped = stopped_response['taskArns']
                print ("\n **************************")
                print ("\n Finished running all tasks in {} minutes.".format(end - start))
                print("\n Finished tasks: {}".format(stopped))

                stopped_statuses = client.describe_tasks(
                    cluster=CLUSTER,
                    tasks=stopped
                )

                for task in stopped_statuses['tasks']:
                    print("\nSuccess: {} \n - {}".format(task['taskArn'], task['overrides']['containerOverrides']))
                for task in stopped_statuses['failures']:
                    print("\nFailure: {} \n - {} \n - {}".format(task['arn'], task['reason'], task['detail']))
            
            print("({}s elapsed) Tasks left: {}/{}".format(int(end-start), len(waiting), total_tasks))





def call_task(client, model, location):
    ## TODO pass parameters??
    task_response = client.run_task(
        taskDefinition=taskDefinition,
        launchType='FARGATE',
        cluster=CLUSTER,
        platformVersion='LATEST',
        count=1,
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [
                    'subnet-0183fe050d93b845a',
                ],
                'assignPublicIp': 'ENABLED',
                'securityGroups': ["sg-0a6e23d1e06d90604"]
            }
        },
        overrides={'containerOverrides': [
            {
                'name': 'climate',
                'command': build_container_command(model, location)
            }
        ]}
    )

    print("ECS Task {} running......".format(task_response))


def build_container_command(model, location):
    cmd = "/bin/sh -c \"cd /app && ls -altr && pwd && python3 --version && pip freeze "
    cmd = cmd + " && " + "/scripts/default.sh cca s3://climate-ensembling/models/{}/ {} {}".format(model, model, location)
    cmd = cmd + "\""
    return [cmd]

ecs_client = boto3.client("ecs")
# Call docker
taskDefinition = "ClimateTaskCF"

models = ["EC-Earth3"]
locations = ["Dhaka"]

for model in models:
    for location in locations:
        call_task(ecs_client, model, location)

monitor_tasks(ecs_client, models, locations)

    # "ephemeralStorage": "200",
