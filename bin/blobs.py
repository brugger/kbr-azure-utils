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






def main() -> None:

    connect(Subscription_Id=os.getenv("AZURE_SUBSCRIPTION", None))
    

    account = 'neuromics'
    cs = connection.storage_client.blob_containers.list('for-neuro-sysmed-shared-storage', account)
    for c in cs:
        print( c.name )
        cc = ContainerClient.from_container_url(f"https://{account}.blob.core.windows.net/{c.name}", credential=connection.credential)

#        print( cc.container_name )


#        for blob in cc.list_blobs(include=[]):
            
#            pp.pprint( blob )
#        for blob in cc.walk_blobs():
#            print( type( blob.blob_tier. ))
#            if isinstance(blob, BlobPrefix):
#                print(f"DIR: {blob.container}/{blob.name}/")
#            else:
#                print(f"{blob.container}/{blob.name} {blob.last_modified} {blob.size} {blob.blob_tier} {blob.blob_tier_change_time}")



        depth = 1
        separator = '   '
        hot_size = 0
        hot_files = 0
        cool_size = 0
        cool_files = 0

        def walk_blob_hierarchy(container_client, prefix=""):
            nonlocal depth, hot_files, hot_size, cool_files, cool_size
            for item in container_client.walk_blobs(name_starts_with=prefix):
                short_name = item.name[len(prefix):]
                if isinstance(item, BlobPrefix):
                    print('Folder: ' + separator * depth + short_name)
                    depth += 1
                    walk_blob_hierarchy(container_client, prefix=item.name)
                    depth -= 1
                else:
                    bc = BlobClient.from_blob_url(f"https://{account}.blob.core.windows.net/{c.name}/{item.name}", credential=connection.credential)
#                    print( bc.get_blob_properties() )
#                    bc.set_standard_blob_tier(standard_blob_tier="Hot")
                    if item.blob_tier == 'Cool':
                        cool_size += item.size
                        cool_files += 1
                    elif item.blob_tier == 'Hot':
                        hot_size += item.size
                        hot_files += 1

                    message = 'Blob: ' + separator * depth + short_name + f"\t{item.size}\t{item.blob_tier} {item.last_modified}"
                    results = list(container_client.list_blobs(name_starts_with=item.name, include=['snapshots']))
                    
                    num_snapshots = len(results) - 1
                    if num_snapshots:
                        message += " ({} snapshots)".format(num_snapshots)
                    print(message)


        walk_blob_hierarchy(cc)
        print( c.name )
        print(f"Hot files : {hot_files} --> {hot_size}b")
        print(f"Cool files : {cool_files} --> {cool_size}b")



#            sys.exit()
#        print( c )




if __name__ == "__main__":
    main()
