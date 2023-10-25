import json
from prefect import task, Flow
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
import os
import subprocess

dbt_graph = None
def write_to_file(lines, filename):
    with open(filename, "w") as f:
        f.write("\n".join(lines))

@task(log_stdout=True)
def run_file(filename):
    subprocess.run(f"python3 {filename}", shell=True)

@task(log_stdout=True)
def generate_dynamic_flow(full_new_flow_name, z):
    
    lines = []
    
    lines.append("from prefect import task, Flow")
    lines.append("from prefect.tasks.prefect import create_flow_run, wait_for_flow_run")
    lines.append("import os")
    lines.append("")
    
    #example creating additional task
    # lines.append(f"@task(name='run_flow_dbt_staging', log_stdout=True)")
    # lines.append(f"def run_flow_dbt_staging():")
    # lines.append(f"    print('running run_flow_dbt_staging')")
    
    lines.append('')
    
    for model in dbt_graph:
        lines.append(f"@task(name='{model['unique_id'].replace('-', '_').replace('.', '_')}', log_stdout=True)")
        lines.append(f"def {model['unique_id'].replace('-', '_').replace('.', '_')}():")
        lines.append(f"    os.system(f'dbt run --models {model['unique_id']}')")
        lines.append('')
    
    lines.append("")
    lines.append("with Flow('GenesisDBTDynamicFlow') as flow:")
    #if you need to run additional flows you can add them like this
    lines.append(f'    flow_run_dbt_staging = create_flow_run(flow_name="Load_DBT_Staging", project_name="myprojectname", task_args=dict(name="Load_DBT_Staging"))')
    lines.append(f'    wait_for_flow_run_dbt_staging = wait_for_flow_run(flow_run_dbt_staging, raise_final_state=True, task_args=dict(name="Wait for Load_DBT_Staging"))')
    
    for model in dbt_graph:
        task_name = model['unique_id'].replace('-', '_').replace('.', '_')
        
        for dependency in model["depends_on"]['nodes']:
            #exclude all non-model stuff
            if dependency.startswith('model.'):
                lines.append(f"    {task_name}.set_upstream({dependency.replace('-', '_').replace('.', '_')})")
            #if you have some kind of a staging task, you can make it first to run
            elif dependency.startswith('source.'):
                lines.append(f"    {task_name}.set_upstream(wait_for_flow_run_dbt_staging)")
            #custom dependencies
            #elif 'your model unique_id that depends on some non-dbt task' in dependency:
            #    #you should create new non-dbt task like in line 34
            #    lines.append(f"    {task_name}.set_upstream(non-dbt task)")
            
        lines.append('')
    
    # flow run and register
    lines.append("flow.run()")
    lines.append("flow.register(project_name='myprojectname')")
    
    write_to_file(lines, full_new_flow_name)
    
@task(log_stdout=True)
def generate_dependencies_json_file(full_path):
    #activate dbt environment and cd into this folder
    c = f'cd /opt/etl/venv/v_dbt && source bin/activate && cd {full_path} '
    #run command to generate dependencies.json in dbt folder
    c += '&& dbt ls --resource-type model --output json > dependencies.json'
    cmd = f'''
    bash -c "{c}"
    '''
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
    stdout, stderr = process.communicate()
    
    if process.returncode != 0:
        raise Exception(f'ERROR generating dependencies.json - {stderr}')
    
@task(log_stdout=True)
def clean_dependencies_json(full_path, z):
    with open(os.path.join(full_path, 'dependencies.json'), 'r') as f:
        lines = f.readlines()
        
        global dbt_graph
        dbt_graph = [json.loads(line) for line in lines if line.strip().startswith('{')]

with Flow("GenesisDBTFather") as flow:
    #path to dbt project
    full_dbt_path = '/mnt/dwh/dbt_project'
    #fullpath to new flow
    full_new_flow_name = '/opt/etl/GenesisDBTDynamicFlow.py'
    
    a = generate_dependencies_json_file(full_dbt_path)
    #by default dbt makes shitty json and we need to clean it first
    b = clean_dependencies_json(full_dbt_path, a)
    c = generate_dynamic_flow(full_new_flow_name, b)
    
    #you can run this new flow from here or later or never
    #d = run_file(full_new_flow_name, c)
    
flow.run()
flow.register(project_name='myprojectname')