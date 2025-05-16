import os
from azure.identity import DefaultAzureCredential
from azure.appconfiguration.provider import load
# Fetch the Application config Value for the given key from Azure App Configuration
def get_azure_appconfig_value(strKeyToFetch):
    endpoint = os.environ.get("AZURE_APPCONFIG_ENDPOINT")
    if endpoint is None:
        raise ValueError("AZURE_APPCONFIG_ENDPOINT environment variable is not set.")

    print(f"Connecting to Azure App Configuration...{endpoint}")

    # Connect to Azure App Configuration using Microsoft Entra ID.
    credential = DefaultAzureCredential()
    configs_list = load(endpoint=endpoint, credential=credential)
    connstr_sb = configs_list[strKeyToFetch]
    if connstr_sb is None:
        raise ValueError(f"Connection string for {strKeyToFetch} not found in Azure App Configuration.")
    print(f"Connection string is {connstr_sb}")
    return connstr_sb