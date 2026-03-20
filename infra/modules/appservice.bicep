@description('Base name for resources')
param baseName string

@description('Environment')
param env string

@description('Azure region')
param location string

@description('Log Analytics ID')
param logAnalyticsId string

@description('Container Registry name')
param acrName string

@description('Application Insights connection string for backend telemetry')
param appInsightsConnectionString string = ''

var backendAppName = 'app-${baseName}-backend-${env}'
var frontendAppName = 'app-${baseName}-frontend-${env}'
var appServicePlanName = 'plan-${baseName}-${env}'

resource appServicePlan 'Microsoft.Web/serverfarms@2023-12-01' = {
  name: appServicePlanName
  location: location
  sku: {
    name: 'B1'
    tier: 'Basic'
    capacity: 1
  }
  kind: 'linux'
  properties: {
    reserved: true
  }
}

resource backendApp 'Microsoft.Web/sites@2023-12-01' = {
  name: backendAppName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: appServicePlan.id
    siteConfig: {
      linuxFxVersion: 'DOCKER|nginx:alpine'
      // ACR credentials are configured post-deploy by the workflow using admin keys.
      // acrUseManagedIdentityCreds is intentionally omitted: setting it at creation
      // time causes an ARM internal server error because the AcrPull role assignment
      // (which depends on the app's principalId) does not yet exist when ARM tries
      // to validate the managed-identity → ACR connection.
      appSettings: [
        {
          name: 'WEBSITES_ENABLE_APP_SERVICE_STORAGE'
          value: 'false'
        }
        {
          name: 'DOCKER_REGISTRY_SERVER_URL'
          value: 'https://${acrName}.azurecr.io'
        }
        {
          name: 'DOCKER_ENABLE_CI'
          value: 'true'
        }
        {
          // Populated by GitHub Actions post-deploy step
          name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
          value: appInsightsConnectionString
        }
      ]
    }
    httpsOnly: true
  }
}

resource frontendApp 'Microsoft.Web/sites@2023-12-01' = {
  name: frontendAppName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: appServicePlan.id
    siteConfig: {
      linuxFxVersion: 'DOCKER|nginx:alpine'
      // See comment on backendApp — acrUseManagedIdentityCreds omitted intentionally.
      appSettings: [
        {
          name: 'WEBSITES_ENABLE_APP_SERVICE_STORAGE'
          value: 'false'
        }
        {
          name: 'DOCKER_REGISTRY_SERVER_URL'
          value: 'https://${acrName}.azurecr.io'
        }
        {
          name: 'DOCKER_ENABLE_CI'
          value: 'true'
        }
      ]
    }
    httpsOnly: true
  }
}

// AcrPull role assignments are intentionally removed.
// The workflow configures ACR access post-deploy using admin credentials
// (--docker-registry-server-user / --docker-registry-server-password in
// az webapp config container set), which is correct for a Basic SKU ACR.
// Managed-identity-based pulls (acrUseManagedIdentityCreds) require the
// role assignment to exist before ARM validates the App Service, creating
// an unresolvable circular dependency that produces an ARM internal server error.

output backendAppName string = backendApp.name
output frontendAppName string = frontendApp.name
output backendAppPrincipalId string = backendApp.identity.principalId
output frontendAppPrincipalId string = frontendApp.identity.principalId
