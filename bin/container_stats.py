#!/usr/bin/env python3

import argparse
import sys
import os
import pprint as pp



from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.common.credentials import ServicePrincipalCredentials
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobPrefix, BlobClient


from munch import Munch, munchify



import kbr.config_utils as config_utils
import kbr.log_utils as logger
import kbr.version_utils as version_utils
import kbr.args_utils as args_utils
import kbr.string_utils as string_utils
import kbr.file_utils as file_utils
import kbr.type_utils as type_utils

sys.path.append(".")

connection = Munch( {})


def connect(Subscription_Id:str) -> None:
    
    
    connection.credential = AzureCliCredential()
    connection.subscription_id = Subscription_Id
    connection.resource_client = ResourceManagementClient(connection.credential, Subscription_Id)
    connection.compute_client  = ComputeManagementClient(connection.credential, Subscription_Id)
    connection.network_client  = NetworkManagementClient(connection.credential, Subscription_Id)
    connection.storage_client  = StorageManagementClient(connection.credential, Subscription_Id)


def container_stats(account:str, name:str, prefix:str="") -> None:

    cc = ContainerClient.from_container_url(f"https://{account}.blob.core.windows.net/{name}", credential=connection.credential)

    hot_size   = 0
    hot_files  = 0
    cool_size  = 0
    cool_files = 0
    snapshots  = 0

    def walk_blob_hierarchy(container_client, prefix=""):
        nonlocal hot_files, hot_size, cool_files, cool_size, snapshots
        for blob in container_client.walk_blobs(name_starts_with=prefix):
            if isinstance(blob, BlobPrefix):
                walk_blob_hierarchy(container_client, prefix=blob.name)
            else:
                bc = BlobClient.from_blob_url(f"https://{account}.blob.core.windows.net/{name}/{blob.name}", credential=connection.credential)
                if blob.blob_tier == 'Cool':
                    cool_size += blob.size
                    cool_files += 1
                elif blob.blob_tier == 'Hot':
                    hot_size += blob.size
                    hot_files += 1

                results = list(container_client.list_blobs(name_starts_with=blob.name, include=['snapshots']))                    
                snapshots += len(results) - 1


    walk_blob_hierarchy(cc, prefix)


    print(f"{name:20} {hot_files:5} {string_utils.readable_bytes(hot_size):>10}    {cool_files:5} {string_utils.readable_bytes(cool_size):>10}")


def container_list_blobs(account:str, name:str, prefix:str="") -> None:

    cc = ContainerClient.from_container_url(f"https://{account}.blob.core.windows.net/{name}", credential=connection.credential)

    depth = 1
    separator = '   '

    def walk_blob_hierarchy(container_client, prefix=""):
        nonlocal depth
        for item in container_client.walk_blobs(name_starts_with=prefix):
            short_name = item.name[len(prefix):]
            if isinstance(item, BlobPrefix):
                print(f"{name}:{item.name}")
                walk_blob_hierarchy(container_client, prefix=item.name)
            else:
#                bc = BlobClient.from_blob_url(f"https://{account}.blob.core.windows.net/{c.name}/{item.name}", credential=connection.credential)
#                print( bc.get_blob_properties() )
#                bc.set_standard_blob_tier(standard_blob_tier="Hot")

                results = list(container_client.list_blobs(name_starts_with=item.name, include=['snapshots']))
                    
                num_snapshots = len(results) - 1
                print(f"{name}:{prefix}{item.name} \t{string_utils.readable_bytes(item.size)}\t{item.blob_tier} {item.last_modified} {num_snapshots}")


    walk_blob_hierarchy(cc, prefix)


def main() -> None:



    parser = argparse.ArgumentParser(description=f'container_stats.py: reports hot/cold files and useage in a storage account/container')


    parser.add_argument('-a', '--account', help="Account name", required=True)
    parser.add_argument('-c', '--container', help="Account name")
    parser.add_argument('-p', '--path', help="sub-path only reporting", default=None)
    parser.add_argument('-l', '--list-blobs', help="Show size/state for all blobs", default=False, action="store_true")

    parser.add_argument('-r', '--resource-group', help="resource group owning the accounts", 
                               default=args_utils.get_env_var('AZURE_RESOURCE_GROUP', None))
    parser.add_argument('-s', '--subscription-id', help="AZURE_SUBSCRIPTION id",
                        default=args_utils.get_env_var('AZURE_SUBSCRIPTION', None))


    args = parser.parse_args()

    connect(Subscription_Id=args.subscription_id)
    
    account = args.account
    resource_group = args.resource_group

    containers = connection.storage_client.blob_containers.list(resource_group, account)
    for c in containers:
        if args.list_blobs:
            if args.container is not None:
                if c.name == args.container:
                    container_list_blobs(account, c.name, args.path )
            else:
                container_list_blobs(account, c.name, args.path )

        else: 
            print("Container       |           Hot        |         Cool")
            print("=====================================================")
            if args.container is not None:
                if c.name == args.container:
                    container_stats(account, c.name, args.path )
            else:
                container_stats(account, c.name, args.path )





if __name__ == "__main__":
    main()
