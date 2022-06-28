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

models = ["EC-Earth3", "GFDL_ESM4", "UK-ESM"]
locations = ['Perth', 'Salvador', 'Mumbai', 'Hyderabad', 'Chennai', 'Shanghai', 'Hong Kong', 'Chengdu', 'Abu Dhabi', 'Abuja', 'Accra', 'Adamstown', 'Addis Ababa', 'Aden', 'Albany', 'Algiers', 'Alofi', 'Amman', 'Andorra la Vella', 'Ankara',  'Antananarivo', 'Apia', 'Ashgabat', 'Asmara', 'Asunción', 'Athens', 'Atlanta', 'Augusta', 'Austin', 'Avarua', 'Baghdad', 'Baku', 'Bamako', 'Bandar Seri Begawan', 'Bangkok', 'Bangui', 'Banjul', 'Basseterre', 'Baton Rouge', 'Beijing', 'Beirut', 'Belfast', 'Belgrade', 'Belmopan', 'Berlin', 'Bern', 'Bishkek', 'Bismarck', 'Bissau', 'Bogotá', 'Boise', 'Boston', 'Brades', 'Brasília', 'Bratislava', 'Brazzaville', 'Bridgetown', 'Brussels', 'Bucharest', 'Budapest', 'Buenos Aires', 'Bujumbura', 'Cairo', 'Calgary', 'Cambridge', 'Camp Thunder Cove', 'Canberra', 'Caracas', 'Cardiff', 'Carson City', 'Castries', 'Cedar City', 'Charleston', 'Charlotte Amalie', 'Charlottetown', 'Cheyenne', 'Chișinău', 'Cockburn Town', 'Colombo', 'Columbia', 'Columbus', 'Conakry', 'Concord', 'Copenhagen', 'Cotonou', 'Dakar', 'Damascus', 'Dar es Salaam', 'Denver', 'Des Moines', 'Dhaka', 'Dili', 'Djibouti', 'Doha', 'Donauwörth', 'Douglas', 'Dover', 'Dublin', 'Dushanbe', 'Edinburgh', 'Flying Fish Cove', 'Frankfort', 'Freetown', 'Funafuti', 'Gaborone', 'George Town', 'Georgetown', 'Gibraltar', 'Guatemala City', 'Gustavia', 'Hagåtña', 'Halifax', 'Hamilton', 'Hanoi', 'Harare', 'Hargeisa', 'Harrisburg', 'Hartford', 'Havana', 'Helena', 'Helsinki', 'Honiara', 'Honolulu', 'Indianapolis', 'Islamabad', 'Jackson', 'Jakarta', 'Jamestown', 'Jefferson City', 'Jerusalem', 'Juba', 'Juneau', 'Kabul', 'Kampala', 'Kathmandu', 'Khartoum', 'Kigali', 'King Edward Point', 'Kingston', 'Kingstown', 'Kinshasa', 'Kuala Lumpur', 'Kuwait City', 'Kyiv', 'La Paz', 'Lansing', 'Libreville', 'Lilongwe', 'Lima', 'Lincoln', 'Lisbon', 'Little Rock', 'Ljubljana', 'Lobamba', 'Lomé', 'London', 'Luanda', 'Lusaka', 'Luxembourg', 'Madison', 'Madrid', 'Majuro', 'Malabo', 'Malé', 'Managua', 'Manama', 'Manila', 'Maputo', 'Mariehamn', 'Marigot', 'Maseru', 'Mata Utu', 'Mexico City', 'Minsk', 'Mogadishu', 'Monaco', 'Moncton', 'Monrovia', 'Montevideo', 'Montgomery', 'Montpelier', 'Montreal', 'Moroni', 'Moscow', 'Munich', 'Muscat', "N'Djamena", 'Nairobi', 'Nashville', 'Nassau', 'Naypyidaw', 'New Delhi', 'Ngerulmud', 'Niamey', 'Nicosia', 'Nouakchott', 'Nouméa', 'Nur-Sultan', 'Nuuk', 'Oklahoma City', 'Olympia', 'Oranjestad', 'Oslo', 'Ottawa', 'Ouagadougou', 'Pago Pago', 'Palikir', 'Panama City', 'Papeete', 'Paramaribo', 'Paris', 'Philipsburg', 'Phnom Penh', 'Phoenix', 'Pierre', 'Podgorica', 'Port Louis', 'Port Moresby', 'Port Vila', 'Port of Spain', 'Port-au-Prince', 'Prague', 'Praia', 'Pretoria', 'Pristina', 'Providence', 'Pyongyang', 'Quito', 'Rabat', 'Raleigh', 'Ramallah', 'Reykjavík', 'Richmond', 'Riga', 'Riyadh', 'Road Town', 'Rome', 'Roseau', 'Sacramento', 'Saint Paul', 'Saipan', 'Salem', 'Salt Lake City', 'San José', 'San Juan', 'San Marino', 'San Salvador', 'Santa Fe', 'Santiago', 'Santo Domingo', 'Sarajevo', 'Saskatoon', 'Seoul', 'Singapore', 'Skopje', 'Sofia', 'South Tarawa', 'Springfield', 'Stanley', 'Stepanakert', 'Stockholm', 'Sukhumi', 'Suva', 'São Tomé', 'Taipei', 'Tallahassee', 'Tallinn', 'Tashkent', 'Tbilisi', 'Tegucigalpa', 'Tehran', 'The Valley', 'Thimphu', 'Tifariti', 'Tirana', 'Tiraspol', 'Tokyo', 'Tomah', 'Topeka', 'Toronto', 'Trenton', 'Tripoli', 'Tskhinvali', 'Tunis', 'Tórshavn', 'Ulaanbaatar', 'Vaduz', 'Valletta', 'Vancouver', 'Vatican City', 'Victoria', 'Vienna', 'Vientiane', 'Vilnius', 'Warsaw', 'Washington', 'Wellington', 'West Island', 'Willemstad', 'Windhoek', 'Winnipeg', 'Yamoussoukro', 'Yaoundé', 'Yaren', 'Yerevan', 'Zagreb']
# locations = ["Dhaka", "Cambridge", "Cardiff", "Perth", "Munich", "San Francisco", "Nairobi", "Tokyo"]
# models = ["EC-Earth3"]
# locations = ["Dhaka"]

for model in models:
    for location in locations:
        call_task(ecs_client, model, location)

monitor_tasks(ecs_client, models, locations)

    # "ephemeralStorage": "200",
