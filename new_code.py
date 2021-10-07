import requests
import json
import os
import sys
from google.cloud import storage
from googleapiclient import discovery
import googleapiclient
from oauth2client.client import GoogleCredentials
import time
from slack_webhook import Slack

from google.auth.transport.urllib3 import AuthorizedHttp
from google.auth.transport.requests import AuthorizedSession

#slack_webhook = "https://hooks.slack.com/services/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"
slack_webhook = "https://hooks.slack.com/services/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"

def send_slack_alert(slack_url = slack_webhook, message=''):
    try:
        slack = Slack(url=slack_url)
        slack.post(text=message)
        print("Successfully sent slack message")
    except Exception as e:
        print("Failed to send slack message  {}:".format(e))

def run_lh_wrkr_cloud_function(param):
    try:
      #url = "https://us-central1-quattr-data-engineering.cloudfunctions.net/development-lighthouse-fetch-site-usability-pages"
      url = "https://us-central1-quattr-data-engineering.cloudfunctions.net/prod-lighthouse-fetch-site-usability-pages"

      # param = {"first":"Mae Carol", 
      #          "last": "Jemison"}


      #r = requests.post(url, data=json.dumps(param))02
      #r = requests.post(url, data=param)
      r = requests.post(url, json=param)

      print(r.json())
      dict = r.json()
      return dict
    except Exception as e:
      send_slack_alert(slack_webhook, str(e))
      send_slack_alert(slack_webhook, message = "failed calling the lh-worker cloud function")
      sys.exit(1)

def get_file_list_inbucket(bucket_name, prefix_name, project_name = "quattr-data-engineering"):
    try:
        client = storage.Client()
        #bucket = storage.Bucket(client, bucket_name, user_project=project_name)
        bucket = client.bucket(bucket_name, user_project=project_name)
        #all_blobs = list(client.list_blobs(bucket))
        all_blobs = list(bucket.list_blobs(prefix=prefix_name))
        
        return len(all_blobs)
    except Exception as e:
        print(str(e))
        print('bucket not found')
        #send_slack_alert(slack_webhook, message = "bucket_not_found {}".format(bucket_name))
        return 0
        #sys.exit(1)
    
def monitoring_cloud_storage(dict):   
    for i in dict.keys():
        #print(i)
        #print(dict[i][0]['no_of_pages'])
        send_slack_alert(slack_webhook, message = "number of pages :" + str(dict[i][0]['no_of_pages']))
        #print(dict[i][0]['bucket_location'])
        send_slack_alert(slack_webhook, message = "bucket location :" + str(dict[i][0]['bucket_location']))
        while(True):
            no_of_files = 0
            devices = ['mobile', 'desktop']
            networks = ['fast', 'medium', 'slow']
            for j in devices:
                for k in networks:
                    bucket_name = dict[i][0]['bucket_location'] + str(j) + '/' + str(k) 
                    #print(dict[i][0]['bucket_location'])
                    uri_list = str(dict[i][0]['bucket_location']).split('/')
                    #print(uri_list)
                    storage_name = uri_list[0]
                    prefix = ''
                    for l in range(1, len(uri_list)-1):
                        prefix += str(uri_list[l]) + '/'
                    prefix += str(j) + '/' + str(k)
                    # print(storage_name)
                    # print(prefix)
                    r = get_file_list_inbucket(storage_name, prefix)
                    #print(r)
                    no_of_files += int(r)
            if no_of_files >= dict[i][0]['no_of_pages']:
                break
            else:
                print('enough no of files not found for customer : {}'.format(str(i)))
                send_slack_alert(slack_webhook, message='enough no of files not found for customer : {}'.format(str(i)))
            time.sleep(60)
        print('all the files found for the customer : {}'.format(str(i)))
        print('\n')
    


#instance = "lh-load-data-01"
def delete_instance(compute, project, zone, name):
    return compute.instances().delete(
        project=project,
        zone=zone,
        instance=name).execute()
    
def stop_instance(compute, project, zone, name):
    return compute.instances().stop(project=project, zone=zone, instance=name).execute()

def wait_for_operation(compute, project, zone, operation):
    print('Waiting for operation to finish...')
    while True:
        result = compute.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation).execute()

        if result['status'] == 'DONE':
            print("done.")
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)

def list_instances(compute, project, zone):
    result = compute.instances().list(project=project, zone=zone).execute()
    return result['items'] if 'items' in result else None

def create_instance(compute, project, zone, name, bucket = ''):
    # Get the latest Debian Jessie image.
    # image_response = compute.images().getFromFamily(
    #     project='debian-cloud', family='debian-9').execute()
    # source_disk_image = image_response['selfLink']

    # Configure the machine
    # machine_type = "zones/%s/machineTypes/n1-standard-1" % zone
    # startup_script = open(
    #     os.path.join(
    #         os.path.dirname(__file__), 'startup-script.sh'), 'r').read()
    # image_url = "http://storage.googleapis.com/gce-demo-input/photo.jpg"
    # image_caption = "Ready for dessert?"

    config = {
  "kind": "compute#instance",
  "name": name,
  "zone": "projects/quattr-data-engineering/zones/us-central1-a",
  "machineType": "projects/quattr-data-engineering/zones/us-central1-a/machineTypes/e2-medium",
  "displayDevice": {
    "enableDisplay": "false"
  },
  "metadata": {
    "kind": "compute#metadata",
    "items": [
      {
        "key": "gce-container-declaration",
        "value": "spec:\n  containers:\n    - name: " + name + "\n      image: >-\n        us-west1-docker.pkg.dev/quattr-data-engineering/load-lh-data-repo/quickstart-image:tag1\n      stdin: false\n      tty: false\n  restartPolicy: Never\n\n# This container declaration format is not public API and may change without notice. Please\n# use gcloud command-line tool or Google Cloud Console to run Containers on Google Compute Engine."
      },
      {
        "key": "google-logging-enabled",
        "value": "true"
      }
    ]
  },
  "tags": {
    "items": []
  },
  "disks": [
    {
      "kind": "compute#attachedDisk",
      "type": "PERSISTENT",
      "boot": "true",
      "mode": "READ_WRITE",
      "autoDelete": "true",
      "deviceName": name,
      "initializeParams": {
        "sourceImage": "projects/cos-cloud/global/images/cos-stable-89-16108-534-2",
        "diskType": "projects/quattr-data-engineering/zones/us-central1-a/diskTypes/pd-balanced",
        "diskSizeGb": "10"
      },
      "diskEncryptionKey": {}
    }
  ],
  "canIpForward": "false",
  "networkInterfaces": [
    {
      "kind": "compute#networkInterface",
      "subnetwork": "projects/quattr-data-engineering/regions/us-central1/subnetworks/default",
      "accessConfigs": [
        {
          "kind": "compute#accessConfig",
          "name": "External NAT",
          "type": "ONE_TO_ONE_NAT",
          "natIP": "34.133.130.172",
          "networkTier": "PREMIUM"
        }
      ],
      "aliasIpRanges": []
    }
  ],
  "description": "",
  "labels": {
    "container-vm": "cos-stable-89-16108-534-2"
  },
  "scheduling": {
    "preemptible": "false",
    "onHostMaintenance": "MIGRATE",
    "automaticRestart": "true",
    "nodeAffinities": []
  },
  "deletionProtection": "false",
  "reservationAffinity": {
    "consumeReservationType": "ANY_RESERVATION"
  },
  "serviceAccounts": [
    {
      "email": "613703970686-compute@developer.gserviceaccount.com",
      "scopes": [
        "https://www.googleapis.com/auth/devstorage.read_only",
        "https://www.googleapis.com/auth/logging.write",
        "https://www.googleapis.com/auth/monitoring.write",
        "https://www.googleapis.com/auth/servicecontrol",
        "https://www.googleapis.com/auth/service.management.readonly",
        "https://www.googleapis.com/auth/trace.append"
      ]
    }
  ],
  "shieldedInstanceConfig": {
    "enableSecureBoot": "false",
    "enableVtpm": "true",
    "enableIntegrityMonitoring": "true"
  },
  "confidentialInstanceConfig": {
    "enableConfidentialCompute": "false"
  }
}




    return compute.instances().insert(
        project=project,
        zone=zone,
        body=config).execute()


def main():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'
    credentials = GoogleCredentials.get_application_default()
    compute = discovery.build('compute', 'v1', credentials= credentials)
    
    customers = "AMEX"
    
    send_slack_alert(slack_webhook, "Started onboarding for {}".format(customers))
    
    param = {"pipeline_type": 1,
        "customer_list": customers
}

    
    dict = run_lh_wrkr_cloud_function(param)

    #time.sleep(120)
    monitoring_cloud_storage(dict)
    
    send_slack_alert(slack_webhook, "all files were created")


    project = 'quattr-data-engineering'  
    zone = 'us-central1-a' 

    print('Creating instance.')

    name = "lh-worker-weekly-run-01"
    send_slack_alert(slack_webhook, message= "Starting Compute engine instance named {}".format(name))
    operation = create_instance(compute, project, zone, name)

    wait_for_operation(compute, project, zone, operation['name'])

    instances = list_instances(compute, project, zone)

    print('Instances in project %s and zone %s:' % (project, zone))
    for instance in instances:
        print(' - ' + instance['name'])

    print("""
    Instance created.
    It will take a minute or two for the instance to complete work.
    """)
    
    send_slack_alert(slack_webhook, message = "Instance created")

    time.sleep(220)
    url1 = "http://34.133.130.172:8080"
    param = {
    "customer_list":customers
    }
    # param = "AMEX"

    r = requests.post(url1, json=param)
    print(r.json())

    # wait = True
    # if wait:
    #     input('press enter to delete the instance.')

    # print('Deleting instance.')

    # operation = delete_instance(compute, project, zone, name)
    # wait_for_operation(compute, project, zone, operation['name'])
    
    # print('stoping the instance {}'.format(name))
    # operation = stop_instance(compute, project, zone, name)
    wait_for_operation(compute, project, zone, operation['name'])
    print('cloud compute completed')
    send_slack_alert(slack_webhook, message = "cloud compute completed successfully")
main()
