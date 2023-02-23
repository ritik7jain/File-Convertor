import json
import boto3
import csv
import yaml
import gzip
def csv_to_yaml(filename):
    with open(filename, "r") as file:
        reader = csv.DictReader(file)
        header = next(reader)
        data = []
        for row in reader:
            policy_data = {}
            for value in header:
                try:
                    policy_data[value] = int(row[value])
                except ValueError:
                    if "[" in row[value]:
                        p = json.loads(row[value])
                        policy_data[value] = p
                    elif "TRUE" in row[value] or "FALSE" in row[value]:
                        policy_data[value] = bool(row[value])
                    else:
                        policy_data[value] = row[value]

            data.append(policy_data)

    with open("/tmp/file3.yml", "w") as yml_file:
        yaml.dump(data, yml_file, Dumper=yaml.SafeDumper, default_style=None, default_flow_style=False, sort_keys=False)

def yaml_to_csv(file_name):
    s3.meta.client.download_file('awstask2', 'policy-confirmance1.yml', '/tmp/policy-confirmance1.yml')
    with open('/tmp/policy-confirmance1.yml', 'r') as inputData:
        full_data = yaml.full_load(inputData)

    data_file = open('output.csv', 'w', newline='')
    csv_output = csv.writer(data_file)

    count = 0

    for data in full_data:
        if count == 0:
            header = data.keys()
            csv_output.writerow(header)
            count += 1

        csv_output.writerow(data.values())

    data_file.close()

def lambda_handler(event, context):
    dn = boto3.resource("dynamodb")
    table =dn.Table("Ritik")
    s3 = boto3.client('s3')
    filename = event['Records'][0]['s3']['object']['key']
    bucketname = event['Records'][0]['s3']['bucket']['name']
    response = s3.get_object(Bucket=bucketname, Key=filename)
    file_contents = response['Body'].read()
    with open('/tmp/myfile.csv', 'wb') as f:
        f.write(file_contents)
    with open('/tmp/myfile.csv', 'r') as f:
        csv_reader = csv.reader(f)
        reader = csv.DictReader(f)
        header = next(reader)
        ls=[]
        for value in header:
            ls.append(value)
        with table.batch_writer() as batch:
            for row in csv_reader:
                item={}
                for i in range(len(ls)):
                    item[ls[i]]=row[i]
                batch.put_item(item
                 
                )
    response = table.scan()
    items_data = response['Items']
    print(items_data[0].keys())
    with open('/tmp/Policy.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(items_data[0].keys())
        print(items_data[0].keys())
        for item_data in items_data:
            writer.writerow(item_data.values())
    csv_to_yaml("/tmp/Policy.csv")
    with open("/tmp/file3.yml", 'rb') as f_in, gzip.open('/tmp/file3.yml.gz', 'wb') as f_out:
        f_out.writelines(f_in)
    s3.upload_file("/tmp/file3.yml.gz","ritikawstask3","ritik.gz")
   # print(event['Records'][0]['s3']['object']['key'])
        
