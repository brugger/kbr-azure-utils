from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.common.credentials import ServicePrincipalCredentials

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


import kbr.log_utils as logger

from munch import Munch, munchify


connection = munchify({})


def id_to_dict(id:str) -> dict:
    res = {}
    fields = id.split("/")
    for i in range(1, len(fields), 2):
      res[ fields[i] ] = fields[i+1]
        
    return res


def check_connection(): # done, needs testing

    if connection.resource_client is None:
        logger.critical("No connection to azure cloud")
        raise ConnectionError

def connect(Subscription_Id:str): # done

    connection.credential = AzureCliCredential()
    connection.subscription_id = Subscription_Id
    connection.resource_client = ResourceManagementClient(connection.credential, Subscription_Id)
    connection.compute_client  = ComputeManagementClient(connection.credential, Subscription_Id)
    connection.network_client  = NetworkManagementClient(connection.credential, Subscription_Id)
    connection.storage_client  = StorageManagementClient(connection.credential, Subscription_Id)

    logger.debug("Connected to azure cloud")

def server_create(name: str, vm_size:str, 
                  network_group:str, compute_group:str, 
                  virtual_network:str, virtual_subnet:str, 
                  admin_username:str, admin_password:str, ssh_key:str , image:str=None, **kwargs) -> None:
  #Done, needs testing and popping args in correctly      

    interface_name = f"{name}-eth0"

    network_interface = connection.network_client.network_interfaces.begin_create_or_update(
                     network_group,
                     interface_name,
                     { 'location': "westeurope",
                       'ip_configurations': [{
                        'name': f'{name}IPconfig',
                        'subnet': {
                          'id': f"/subscriptions/{connection.subscription_id}/resourceGroups/{network_group}/providers/Microsoft.Network/virtualNetworks/{virtual_network}/subnets/{virtual_subnet}",
                        }}
                        ]
                      } 
                    ).result()


#      print( network_interface )

    vm_config = { "location": "westeurope",
                  "hardware_profile": {
                    "vm_size": vm_size
                  },
                  'linux_configuration': {'disable_password_authentication': False,
                                          'patch_settings': {'patch_mode': 'ImageDefault'},
                                          'provision_vm_agent': True},
                  "storage_profile": {
                    "image_reference": {
                      #? "id": "/subscriptions/5a9e26a0-6897-44d6-963e-fae2a2061f27/resourceGroups/FOR-NEURO-SYSMED-UTV-COMPUTE/providers/Microsoft.Compute/images/circ-rna-v1-img"
                      "sku": "8_2",
                      "publisher": "Openlogic",
                      "version": "latest",
                      "offer": "centos"
                    },
                    "os_disk": {
                      "caching": "ReadWrite",
                      "managed_disk": {
                        "storage_account_type": "Standard_LRS"
                      },
                      "name": f"{name}-disk",
                      "create_option": "FromImage"
                    },
                  },
                  "os_profile": {
                    "admin_username": admin_username,
                    "admin_password": admin_password,
                    "computer_name": f"{name}",
                    "linuxConfiguration": {
                      "ssh": {
                        "publicKeys": [
                            {"path": f"/home/{admin_username}/.ssh/authorized_keys",
                             "keyData": f"{ssh_key}"
                            }

                        ]
                      },
                    }
                  },
                  "network_profile": {
                    "network_interfaces": [
                      {"id": f"/subscriptions/{connection.subscription_id}/resourceGroups/{network_group}/providers/Microsoft.Network/networkInterfaces/{interface_name}",
                      "properties": {
                        "primary": True
                      }
                    } 
                  ]
                }
              }

    if image is not None:
        vm_config['storage_profile']['image_reference'] = {'id': image}

    vm = connection.compute_client.virtual_machines.begin_create_or_update(
                compute_group,
                name,
                vm_config
          ).result()

    return vm.id

def servers() -> list: 

    servers = []

    vm_list = connection.compute_client.virtual_machines.list_all()

    for vm_general in vm_list:
        general_view = vm_general.id.split("/")
        resource_group = general_view[4]
        vm_name = general_view[-1]
        vm = connection.compute_client.virtual_machines.get(resource_group, vm_name, expand='instanceView')

        codes = []
        power_state = "unknown"
        provisioning_state = "unknown"
        for stat in vm.instance_view.statuses:
            f = stat.code.split("/")
            k,v = f[0], f[1]
            if k == 'PowerState':
                power_state = v
            elif k == 'ProvisioningState':
                provisioning_state = v

        ips = server_ip(vm.id)

        servers.append({'id': vm.id, 'name': vm.name.lower(), 'status': power_state, 'ip': ips})
          
    logger.debug("Servers: \n{}".format(pp.pformat(servers)))
    return servers


def server(id:str): # done, needs  testing

    id_dict = id_to_dict( id )

    return connection.compute_client.virtual_machines.get(id_dict['resourceGroups'], id_dict['virtualMachines'] )


def server_ip(id: str, ipv: int = 4):

    ips = []
    vm = server(id)

    for network_interface in vm.network_profile.network_interfaces:
        id_dict = id_to_dict( network_interface.id )
        network_interface = connection.network_client.network_interfaces.get(id_dict['resourceGroups'], id_dict['networkInterfaces'])
        for ip in network_interface.ip_configurations: 
            if ip.private_ip_address_version == f"IPv{ipv}":
                ips.append( ip.private_ip_address)

    return ips


def server_names() -> list: # done
    names = []
    for server in servers():
        names.append(server['name'])

    return names

def server_delete(id: str, **kwargs):

    vm = server(id)
    vm_dict = id_to_dict( id )
    request = connection.compute_client.virtual_machines.begin_delete(vm_dict['resourceGroups'], vm_dict['virtualMachines']).result()

    for network_interface in vm.network_profile.network_interfaces:
        network_dict = id_to_dict( network_interface.id )
        connection.network_client.network_interfaces.begin_delete(network_dict['resourceGroups'], network_dict['networkInterfaces'])

    os_disk_name = vm.storage_profile.os_disk.name 
    connection.compute_client.disks.begin_delete(vm_dict['resourceGroups'], os_disk_name)



def server_stop(id: str, compute_group:str, **kwargs): # done, needs testing
    """ stops a server """

    connection.compute_client.virtual_machines.power_off(compute_group, id).result()
    logger.debug("Stopped server id:{}".format(id))


def storage_containers(name:str) -> any:

  containers = connection.storage_client.blob_containers.list('FOR-NEURO-SYSMED-SHARED-STORAGE', name)
  for c in containers:
    print( c )
    print( c.id )
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=c.id, credential=connection.credential )


  
