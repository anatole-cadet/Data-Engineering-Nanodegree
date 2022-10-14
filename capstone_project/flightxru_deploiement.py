import pandas as pd
import boto3
import configparser
import json
import os
import time

welcome_text ="""
===========================================================================================
* Capstone Project: Build ETL for the flightxru of Russia
===========================================================================================
*
* This is going to create the AWS cluster and create the database flightxru_db on Redshift. 
*
===========================================================================================
"""



print(welcome_text)

config = configparser.ConfigParser()
config.optionxform = lambda key_config: key_config.upper()
config.read_file(open('configuration/flightxru.cfg'))


# Get the informations of the config file

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DB_NAME                 = config.get("CLUSTER","DB_NAME")
DB_USER            = config.get("CLUSTER","DB_USER")
DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DB_PORT               = config.get("CLUSTER","DB_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")




os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']


# Create the client ressources.
ec2 = boto3.resource(
    "ec2",
    region_name = "us-west-2",
    aws_access_key_id = KEY,
    aws_secret_access_key = SECRET
)

s3 = boto3.resource(  
    "s3",
    region_name="us-west-2",
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

iam = boto3.client(
    "iam",
    region_name = "us-west-2",
    aws_access_key_id = KEY,
    aws_secret_access_key = SECRET
)

redshift = boto3.client(
    "redshift",
    region_name = "us-west-2",
    aws_access_key_id = KEY,
    aws_secret_access_key = SECRET
)

def start_operations():
    """This function is for starting the program.
    """
    create_iam_role()
    create_cluster()
    open_incomming_tcp_port()
    getting_endpoint_arn_update_config_cfg()
    
    
def create_iam_role():
    """This function is for creating the iam role
    """
    try:
        dwhRole = iam.create_role(
                Path='/',
                RoleName=DWH_IAM_ROLE_NAME,
                Description="Allow the clusters of redshift to interact with the aws services",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")["ResponseMetadata"]["HTTPStatusCode"]


def create_cluster():
    """This function is for creating the cluster and get the IAM role ARN
    """
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    try:
        response = redshift.create_cluster(        
        # add parameters for hardware
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        # add parameters for identifiers & credentials
        DBName=DB_NAME,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DB_USER,
        MasterUserPassword=DB_PASSWORD,

        # add parameter for role (to allow s3 access)
        IamRoles=[roleArn]  
        )
        #print("\tThe cluster {} is beeing creating......".format(DWH_CLUSTER_IDENTIFIER))
    except Exception as e:
        print(e)



def open_incomming_tcp_port():
    """This function is for Opening an incoming TCP port to access the cluster ednpoint
    """    
    try:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0',  
            IpProtocol='TCP',  
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
        #print("Terminé")
    except Exception as e:
        pass #print("\tThe specified rule peer: 0.0.0.0/0, TCP, from port: 5439, to port: 5439,is already exists")
        

        
def getting_endpoint_arn_update_config_cfg():    
    """This function is for getting endpoint and role arn, and Updating the config file.
    """
    try:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        #print("\tPLEASE WAIT..........")

        # Run this until the cluster be available
        while (myClusterProps["ClusterStatus"] != "available"):
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            if myClusterProps["ClusterStatus"] == "available":
                #print("\tThe cluster is now available")
                break;
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

        # Creating a bucket
        
        try:
            list_bucket_exist = []
            bucket_name = "flightxru-bucket"
            s3_client = boto3.client('s3')
            list_of_buckets = s3_client.list_buckets()
            
            for b in list_of_buckets['Buckets']:
                if bucket_name == b['Name']: #in list_of_buckets['Buckets'][0]:
                    list_bucket_exist.append(b['Name'])
            if not(len(list_bucket_exist)> 0):
                s3_client.create_bucket(Bucket=bucket_name,
                                CreateBucketConfiguration={'LocationConstraint':"us-west-2"})
                # os.system(f"aws s3 mb s3://{bucket_name}")
                
        except Exception as e:
            print(e)

        # Updating the config file by writing the endpoint and the arn
        update_config_cluster_host = config['CLUSTER']
        update_config_cluster_host['HOST']= DWH_ENDPOINT

        update_config_cluster_arn = config['IAM_ROLE']
        update_config_cluster_arn['ARN']= DWH_ROLE_ARN

        update_config_cluster_s3 = config['S3']
        update_config_cluster_s3['OTHER_DATA'] = 's3://flightxru-bucket'
        with open('configuration/flightxru.cfg', 'w') as update_data_conf:
            config.write(update_data_conf)
        #print("\tThe config file dwh.cfg was updated for the HOST and the ARN")
    except Exception as e:
        pass #print("\tMaybe the cluster is not available yet")


if __name__ == "__main__":
    #choice = input("Your choice :")
    #if choice == '1':
    print("A- CREATE A CLUSTER AND A BUCKET..................", end='\r')
    start_operations()
    time.sleep(2.4)
    print("A- CREATE A CLUSTER AND A BUCKET................FINISHED")
    
    print("B- CREATE TABLES IN THE flightxru DATABASE.......", end='\r')
    os.system("python queries/create_tables.py")
    time.sleep(2.4)
    print("B- CREATE TABLES IN THE flightxru DATABASE......FINISHED")

    print("\nC- RUN THE ETL................................", end='\r')
    os.system("python flightxru_etl.py")
    time.sleep(2.4)
    print("\nC- RUN THE ETL................................FINISHED")
    
    #print("\n\n---------")
    #print("The ETL was builded and execute withh success")
    
    # Delete the ressources and the redshif cluster and iam role on aws.
    delete_ressources = input("****** Press on 2 to exit to the program \n(when exit to the program, the resources on aws will be deleted):")
    
    if delete_ressources == '2':
        deleted_result = False
        print("Please wait, deletting the ressources...........")
        while (deleted_result==False):
            try:
                redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
                myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
                iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
                iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
                deleted_result = False
            except Exception as e:
                deleted_result = True
                print("Ressources deleted with success\n Bye")
                break
else:
    exit(0)
    