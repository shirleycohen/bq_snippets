import os
import json
import requests
import base64
import yaml
import time
import google.auth
import google.auth.transport.requests
from google.cloud import bigquery
from google.adk.agents import Agent
from dotenv import load_dotenv

load_dotenv()
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
REGION = os.getenv("GOOGLE_CLOUD_LOCATION")
REPO = os.getenv("DATAFORM_REPO")
WORKSPACE = os.getenv("DATAFORM_WORKSPACE")
SERVICE_ACCOUNT_EMAIL = f"developer@{PROJECT}.iam.gserviceaccount.com"
PIPELINE = f"projects/{PROJECT}/locations/{REGION}/repositories/{REPO}/workspaces/{WORKSPACE}"

# this function is called by the agent tools
def get_auth_token():
    creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    # refresh the credentials to get an access token
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token
    

def get_table_listing(project: str, source_dataset: str) -> dict:
    
    """Retrieves the list of tables given a source dataset.

    Args:
        project (str): The project id of the GCP project where the tables are stored. 
        dataset (str): The name of the dataset in BigQuery where the source tables are stored

    Returns:
        dict: A dictionary containing the status and the list of source tables.
    """

    success = True
    source_tables = []
  
    client = bigquery.Client()
    dataset_id = f"{project}.{source_dataset}"

    print(f"Fetching the source tables from the dataset '{dataset_id}':")

    try:
        tables = client.list_tables(dataset_id)
      
        for table in tables:
            print("Found table: ", table.table_id)
            source_tables.append(table.table_id)

    except Exception as e:
        print(f"Error occurred while listing the tables in {source_dataset}: {e}")
        success = False

    return {"status": success, "source_tables": source_tables}


def generate_prompts(source_tables: list) -> dict:
    
    """Generates a unique LLM prompt for each source table. Prompts are then used by the Data Engineering Agent to model the target tables.  

    Args:
        source_tables (list): The list of table names that we want to source from.

    Returns:
        dict: A dictionary containing the status and the list of generated prompts. 
    """
    
    success = True
    prompts = []
    
    for source_table in source_tables:
        
        target_table = source_table.replace("source", "target")
        
        prompt = f"""Given the source table, {PROJECT}.{SOURCE_DATASET}.{source_table}, and the target table, 
                    {PROJECT}.{TARGET_DATASET}.{target_table}, please generate a Dataform pipeline that transforms 
                    the data from the source schema to the target schema."""

        prompts.append(prompt)
        print(f"generated prompt for {source_table} -> {target_table}")

    if len(prompts) == 0: 
        success = False
    
    return {"status": success, "prompts": prompts}


def initialize_dataform_workspace() -> dict:
    """
    Initializes a Dataform workspace by creating the initial files and installing packages.

    Args:
        None

    Returns:
        dict: A dictionary containing the status.
    """

    success = True

    token = get_auth_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    workspace_path = f"projects/{PROJECT}/locations/{REGION}/repositories/{REPO}/workspaces/{WORKSPACE}"
    base_url = f"https://dataform.googleapis.com/v1beta1/{workspace_path}"

    # 1. Create workflow_settings.yaml
    workflow_settings = {
        'defaultProject': PROJECT,
        'defaultLocation': REGION,
        'defaultDataset': 'dataform',
        'defaultAssertionDataset': 'dataform_assertions',
        'dataformCoreVersion': '3.0.16',
    }
    yaml_string = yaml.dump(workflow_settings)
    # The API expects the file content to be a base64 encoded string
    encoded_yaml = base64.b64encode(yaml_string.encode('utf-8')).decode('utf-8')

    write_file_url = f"{base_url}:writeFile"
    write_settings_payload = {
        "path": "workflow_settings.yaml",
        "contents": encoded_yaml
    }

    settings_response = requests.post(write_file_url, headers=headers, json=write_settings_payload)

    if settings_response.status_code == 200:
        print("Successfully wrote workflow_settings.yaml.")
    else:
        print(f"Failed to write workflow_settings.yaml: {settings_response.text}")
        success = False
        return

    # 2. Create .gitignore
    gitignore_content = "node_modules/"
    encoded_gitignore = base64.b64encode(gitignore_content.encode('utf-8')).decode('utf-8')
    write_gitignore_payload = {
        "path": ".gitignore",
        "contents": encoded_gitignore
    }
    gitignore_response = requests.post(write_file_url, headers=headers, json=write_gitignore_payload)

    if gitignore_response.status_code == 200:
        print("Successfully wrote .gitignore.")
    else:
        print(f"Failed to write .gitignore: {gitignore_response.text}")
        success = False
        return

    # 3. Install npm packages
    print("Installing npm packages...")
    install_packages_url = f"{base_url}:installNpmPackages"
    packages_response = requests.post(install_packages_url, headers=headers)

    if packages_response.status_code == 200:
        print("NPM packages installed successfully.")
    else:
        print(f"Failed to install NPM packages: {packages_response.text}")
        success = False
        
    return {"status": success}


def call_data_engineering_agent(prompt: str, instructions: list = []) -> dict:
    
    """
    Calls the BigQuery Data Engineering Agent REST API with a prompt and an optional instruction file.
    
    Args:
        prompt (str): The prompt that will be passed to the Data Engineering Agent
        instructions: (list): The instruction files to be passed to the Data Engineering Agent. This is optional. 

    Returns:
        dict: A dictionary containing the status and message received from the agent.
    """

    success = True

    url = f"https://geminidataanalytics.googleapis.com/v1alpha1/projects/{PROJECT}/locations/global:run"

    token = get_auth_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    data = {
        "pipeline_id": PIPELINE,
        "messages": [{"user_message": {"text": prompt}}],
    }

    # If a list of instructions is provided, format it for the API request
    if len(instructions) > 0:
        agent_instructions_list = [
            {"name": inst.get("name"), "definition": inst.get("definition")}
            for inst in instructions if inst.get("name") and inst.get("definition")
        ]

        if agent_instructions_list:
            data["inline_context"] = {
                "agent_instructions": agent_instructions_list
            }

    response = requests.post(url, headers=headers, json=data, stream=True)
    response.raise_for_status()

    formatted_message = ""
    print("\n--- Agent Response Stream ---")
    for line in response.iter_lines():
        # filter out keep-alive new lines
        if line:
            decoded_line = line.decode("utf-8")
            try:
                # Each line is expected to be a separate JSON message
                message = json.loads(decoded_line)
                formatted_message += json.dumps(message, indent=2)
                print(formatted_message)
            except json.JSONDecodeError:
                # If a line is not valid JSON, print it for debugging
                formatted_message += decoded_line
                success = False
    print("-------------------------\n")
    
    return {"status": success, "message": formatted_message}



def execute_dataform_pipeline()-> dict:
    """
    Compiles a Dataform workspace, executes the pipeline, and waits for it to complete.

    Args:
        None

    Returns:
        dict: A dictionary containing the status and final workflow invocation object if successful.
    """
                              
    success = True

    token = get_auth_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # --- Step 1: Compile the workspace ---
    print("Compiling Dataform workspace...")
    compilation_endpoint = (
        f"https://dataform.googleapis.com/v1/projects/{PROJECT}/"
        f"locations/{REGION}/repositories/{REPO}/compilationResults"
    )
    workspace_resource_name = (
        f"projects/{PROJECT}/locations/{REGION}/"
        f"repositories/{REPO}/workspaces/{WORKSPACE}"
    )
    compilation_body = {"workspace": workspace_resource_name}

    try:
        response = requests.post(compilation_endpoint, headers=headers, json=compilation_body)
        response.raise_for_status()
        compilation_result = response.json()
        compilation_result_name = compilation_result.get("name")
        print(f"Successfully compiled. Result name: {compilation_result_name}")
    except requests.exceptions.HTTPError as e:
        success = False
        msg = "API Error during compilation: {e}\nResponse body: {e.response.text}"
        print(msg)
        return {"status": success, "message": msg}

    # --- Step 2: Execute the compiled result ---
    print("Executing the pipeline...")
    invocation_endpoint = (
        f"https://dataform.googleapis.com/v1/projects/{PROJECT}/"
        f"locations/{REGION}/repositories/{REPO}/workflowInvocations"
    )

    invocation_config = {
        "serviceAccount": SERVICE_ACCOUNT_EMAIL
    }

    # If a target_table_name is provided, include it in the invocationConfig to filter execution by tags.
    #if len(target_table_name) > 0:
        #invocation_config["includedTags"] = [target_table_name]
        #print(f"Executing only actions tagged with: {target_table_name}")

    invocation_body = {
        "compilationResult": compilation_result_name,
        "invocationConfig": invocation_config
    }

    try:
        response = requests.post(invocation_endpoint, headers=headers, json=invocation_body)
        response.raise_for_status()
        workflow_invocation = response.json()
        invocation_name = workflow_invocation.get('name')
        print(f"Successfully started workflow invocation: {invocation_name}")
    except requests.exceptions.HTTPError as e:
        success = False
        msg = f"API Error during execution: {e}\nResponse body: {e.response.text}"
        print(msg)
        return {"status": success, "message": msg}

    # --- Step 3: wait for the execution to complete ---
    print("\nWaiting for execution to complete...")
    status_endpoint = f"https://dataform.googleapis.com/v1/{invocation_name}"

    while True:
        try:
            status_response = requests.get(status_endpoint, headers=headers)
            status_response.raise_for_status()
            invocation_details = status_response.json()
            current_state = invocation_details.get("state")

            print(f"Current state: {current_state}")

            if current_state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                print(f"Execution finished with state: {current_state}")
                if current_state == "SUCCEEDED":
                     print("Pipeline executed successfully.")
                else:
                     print("Pipeline execution did not succeed.")
                return invocation_details

            # wait for 2 seconds before checking the status again
            time.sleep(2)

        except requests.exceptions.HTTPError as e:
            success = False
            msg = f"API Error while checking status: {e}\nResponse body: {e.response.text}"
            print(msg)
            return {"status": success, "message": msg}

    return {"status": success}
            

root_agent = Agent(
    name="schema_mapper_agent",
    model="gemini-2.0-flash",
    description=(
        "Agent that takes a list of source tables in BigQuery and maps them to a set of target tables in BigQuery."
    ),
    instruction=(
        """
        You are a helpful agent who executes a schema mapping workflow which transforms data from source schemas to their target schemas.
        You do this by fetching the source table listing, generating a prompt for each one, initializing the Dataform environment, 
        calling the Data Engineering Agent, and finally running the Dataform pipeline. 
        Always start by greeting the user and letting them know what you can do. Then, ask them if they want to execute the workflow. 
        Return the status from each step of the workflow and check with the user before continuing to the next step. 
        """
    ),
    tools=[get_table_listing, generate_prompts, initialize_dataform_workspace, call_data_engineering_agent, execute_dataform_pipeline],
)