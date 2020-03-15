from multiprocessing import Manager, Process
import time
import numpy as np
from azure.cli.core import get_default_cli
import json

def az_cli(args_str):
    args = args_str.split()
    cli = get_default_cli()
    cli.invoke(args)
    if cli.result.result:
        return cli.result.result
    elif cli.result.error:
        raise cli.result.error
    return True

def create_vm(vm_index, resource_group_name, vm_image, vm_size, vm_creation_time_dict):
    """
    This function creates a vm 
    and return the time from when the vm creation request is sent 
    to when the notification of vm creation is received.
    """

    vm_creation_call = f"vm create --location northcentralus --resource-group {resource_group_name} --name myVM{vm_index} --image {vm_image} --size {vm_size} --output none"

    print(f"myVM{vm_index} creation starts")
    start_time = time.monotonic()
    vm_creation_call_success = az_cli(vm_creation_call)
    end_time = time.monotonic()

    if not vm_creation_call_success:
        print(f"vm_{vm_index} in {resource_group_name} is not successfully created")
        exit()
    
    vm_creation_time = end_time - start_time
    vm_creation_time_dict[vm_index] = vm_creation_time
    return vm_creation_time

def parse_time(deployment_duration):
    time_string = deployment_duration[2:-1]
    if "M" in time_string:
        minute, second = deployment_duration[2:-1].split("M")
        return 60 * float(minute) + float(second)
    else:
        return float(time_string)

if __name__ == '__main__':
    seed = int(input("random seed: "))
    np.random.seed(seed)

    # 0) Initiazlie data - dictionary of Q (= number of VMs to create). 
    # Each value is a dict (key = the index of experiment with Q) of dictinoaries (key - VM index / value - VM creation time) 
    data0 = dict() # VM creation time of type 0: The time duration of the deployment of a VM creation
    data1 = dict() # VM creation time of type 1: The time duration from when a VM creation request is sent to when we are notified of the VM creation
    each_Q_index = dict() # This is to keep track of how many experiments have been done for each Q value
    manager = Manager() # This is to get VM creation time from each process creating a VM   

    error_counter = 0

    for experiment_index in range(500):
        print("-------------------------------------")
        print(f"experiment_index: {experiment_index}")
        # 1) Sample Q 
        Q = np.random.randint(1, 51) 
        print(f"Q: {Q}")
        # 2) Declare variables
        if data1.get(Q) is None:
            data1[Q] = dict()
        if data0.get(Q) is None:
            data0[Q] = dict()

        if each_Q_index.get(Q) is None:
            each_Q_index[Q] = 1
            Q_index = 1
        else:
            each_Q_index[Q] += 1
            Q_index = each_Q_index[Q]

        vm_creation_time_dict1 = manager.dict() # dictinoary of (key = VM index, value = VM creation time)
        resource_group_name = f"Q_{Q}_index_{Q_index}_seed_{seed}"
        vm_image = "UbuntuLTS"
        vm_size = "Standard_DS2_v2" # This has 2 vCPUs and RAM of 7 GiB

        # 3) Create a resource group
        az_cli(f"group create --name {resource_group_name} --location northcentralus --output none")

        # 4) Fire Q number of VM creation calls in parallel
        processes = list()
        print(f"{Q} VM creations start!")
        for vm_index in range(Q):
            p = Process(target=create_vm, args=(vm_index, resource_group_name, vm_image, vm_size, vm_creation_time_dict1))
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

        # 5) Store VM creation time for each VM index
        # 5-1: (type 1) VM creation time that I counted
        data1[Q][Q_index] = vm_creation_time_dict1.copy()

        # 5-2: (type 0) VM deployment time retrieved using Azure CLI (command line API)
        vm_creation_dict0 = dict()

        vm_deployments = az_cli(f"deployment group list -g {resource_group_name} --output none")
        for vm_deployment in vm_deployments:
            vm_properties = vm_deployment['properties']
            deployment_duration = parse_time(vm_properties['duration'])
            output_resources = vm_properties['outputResources']

            for i in range(len(output_resources)):
                if "virtualMachines" in output_resources[i]['id']:
                    vm_name_index_in_output_resources= i
            
            vm_name = (output_resources[vm_name_index_in_output_resources]['id'].split("virtualMachines/"))[1]

            if vm_name[:4] != "myVM":
                print(f"{vm_name} does not have 'myVM'")
                exit()

            vm_index = int(vm_name.replace("myVM", ""))

            vm_creation_dict0[vm_index] = deployment_duration

        data0[Q][Q_index] = vm_creation_dict0

        # 6) Delete the resource group
        print(f"Start deleting the resource group {resource_group_name}")
        az_cli(f"group delete --name {resource_group_name} --yes --output none")

        while True:
            resource_groups = az_cli("group list --output none")
            still_exist = False
            for group in resource_groups:
                if resource_group_name in group['id']:
                    still_exist = True
                    break
            if not still_exist:
                print(f"Deleting {resource_group_name} done.")
                break
            else:
                time.sleep(90)
                
        time.sleep(90)
    
    print(f"\n\n\nerror_counter: {error_counter}")

    # 6) Store the dictionary "data" as a json file
    with open(f"vm_creation_time_seed_{seed}.json", "w") as write_file1:
        json.dump(data1, write_file1)
    with open(f"vm_deployment_time_seed_{seed}.json", "w") as write_file1:
        json.dump(data0, write_file1)
