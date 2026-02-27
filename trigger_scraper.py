import boto3

ecs = boto3.client('ecs')
ec2 = boto3.client('ec2')

print("1. Locating Gestalt Cluster & Task...")
gestalt_cluster = next((c for c in ecs.list_clusters()['clusterArns'] if 'GestaltCluster' in c), None)
scraper_task = next((t for t in ecs.list_task_definitions(sort='DESC')['taskDefinitionArns'] if 'GestaltScraperTask' in t), None)

print("2. Locating AWS Default VPC...")
vpcs = ec2.describe_vpcs(Filters=[{'Name': 'isDefault', 'Values': ['true']}])['Vpcs']
if not vpcs:
    print("CRITICAL: Could not find Default VPC.")
    exit(1)
    
default_vpc = vpcs[0]['VpcId']

print("3. Grabbing Default Subnets...")
subnets = ec2.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [default_vpc]}])['Subnets']
subnet_ids = [s['SubnetId'] for s in subnets]

print("4. Applying Amazon Q's Security Group Rules...")
sg_name = 'GestaltScraperOutboundSG'
sgs = ec2.describe_security_groups(Filters=[{'Name': 'vpc-id', 'Values': [default_vpc]}])['SecurityGroups']
scraper_sg = next((sg for sg in sgs if sg['GroupName'] == sg_name), None)

if not scraper_sg:
    print(" -> Creating new Security Group (AWS auto-adds Allow-All Outbound)...")
    new_sg = ec2.create_security_group(GroupName=sg_name, Description='Allow Outbound HTTPS/HTTP', VpcId=default_vpc)
    sg_id = new_sg['GroupId']
else:
    sg_id = scraper_sg['GroupId']
    print(f" -> Found existing SG: {sg_id}")

print(f"\n5. Launching Fargate Task with Public IP ENABLED...")
response = ecs.run_task(
    cluster=gestalt_cluster,
    taskDefinition=scraper_task,
    launchType='FARGATE',
    networkConfiguration={
        'awsvpcConfiguration': {
            'subnets': subnet_ids,
            'securityGroups': [sg_id],
            'assignPublicIp': 'ENABLED'
        }
    }
)

if response.get('failures'):
    print(f"\nFAILED TO LAUNCH:")
    for failure in response['failures']:
        print(f" - {failure['reason']}")
else:
    task_id = response['tasks'][0]['taskArn'].split('/')[-1]
    print(f"\nSUCCESS! Task is booting up.")
    print(f"Task ID: {task_id}")
    print("Go to the ECS Console -> GestaltCluster -> Tasks tab to watch the logs!")