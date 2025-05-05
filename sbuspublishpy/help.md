# Set user environment variables
setx ASPNETCORE_ENVIRONMENT "Development"
setx AZURE_CLIENT_ID "<your-client-id>"
setx AZURE_TENANT_ID "<your-tenant-id>"
setx AZURE_CLIENT_SECRET "<your-client-secret>"
setx  AZURE_APPCONFIG_ENDPOINT  "https://sunappconfig.azconfig.io"

# Set system environment variables - requires running as admin
setx ASPNETCORE_ENVIRONMENT "Development" /m
setx AZURE_CLIENT_ID "<your-client-id>" /m
setx AZURE_TENANT_ID "<your-tenant-id>" /m
setx AZURE_CLIENT_SECRET "<your-client-secret>" /m

https://learn.microsoft.com/en-us/dotnet/azure/sdk/authentication/local-development-service-principal?tabs=azure-portal%2Cwindows%2Ccommand-line