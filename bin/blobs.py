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
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobPrefix


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

        def walk_blob_hierarchy(container_client, prefix="methylation"):
            nonlocal depth
            for item in container_client.walk_blobs(name_starts_with=prefix):
                short_name = item.name[len(prefix):]
                if isinstance(item, BlobPrefix):
                    print('Folder: ' + separator * depth + short_name)
                    depth += 1
                    walk_blob_hierarchy(container_client, prefix=item.name)
                    depth -= 1
                else:
                    message = 'Blob: ' + separator * depth + short_name + f"\t{blob.size}\t{blob.blob_tier} {blob.last_modified}"
                    results = list(container_client.list_blobs(name_starts_with=item.name, include=['snapshots']))
                    num_snapshots = len(results) - 1
                    if num_snapshots:
                        message += " ({} snapshots)".format(num_snapshots)
                    print(message)
        walk_blob_hierarchy(cc)



#            sys.exit()
#        print( c )




if __name__ == "__main__":
    main()
