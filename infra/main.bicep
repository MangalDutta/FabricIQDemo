targetScope = 'resourceGroup'

@description('Base name for all resources')
param baseName string = 'cust360'

@description('Environment (dev, test, prod)')
param env string = 'dev'

@description('Azure region for resources')
param location string = resourceGroup().location

@description('Enable private endpoints')
param enablePrivateEndpoints bool = false

// ── Optional Fabric Capacity provisioning ────────────────────────────────────
@description('''
Fabric capacity SKU to provision (e.g. F2, F4, F8, F16, F32, F64, F128, F256, F512, F1024, F2048, Trial).
Leave empty to skip capacity provisioning and use an existing capacity via the deploy workflow input.
''')
param fabricSku string = ''

@description('Override the auto-generated Fabric capacity name (optional). Must be lowercase alphanumeric + hyphens, 3-63 chars, globally unique per region.')
param fabricCapacityName string = ''

@description('UPNs / email addresses of Fabric capacity admins (required when fabricSku is set). E.g. ["admin@contoso.com"]')
param fabricCapacityAdmins array = []

var vnetName = 'vnet-${baseName}-${env}'
var logAnalyticsName = 'log-${baseName}-${env}'

// Fabric capacity name: honour override, else generate from baseName + env.
// Fabric requires lowercase alphanumeric/hyphens only, so strip anything else.
var resolvedCapacityName = empty(fabricCapacityName)
  ? 'cap-${toLower(baseName)}-${toLower(env)}'
  : fabricCapacityName

module networking 'modules/networking.bicep' = if (enablePrivateEndpoints) {
  name: 'networking-deployment'
  params: {
    vnetName: vnetName
    location: location
    baseName: baseName
    env: env
  }
}

module monitoring 'modules/monitoring.bicep' = {
  name: 'monitoring-deployment'
  params: {
    logAnalyticsName: logAnalyticsName
    location: location
    baseName: baseName
    env: env
  }
}

module acr 'modules/acr.bicep' = {
  name: 'acr-deployment'
  params: {
    baseName: baseName
    env: env
    location: location
    acrSku: 'Basic'
    privateEndpointSubnetId: enablePrivateEndpoints ? networking.outputs.privateEndpointSubnetId : ''
  }
}

module keyVault 'modules/keyvault.bicep' = {
  name: 'keyvault-deployment'
  params: {
    baseName: baseName
    env: env
    location: location
    privateEndpointSubnetId: enablePrivateEndpoints ? networking.outputs.privateEndpointSubnetId : ''
  }
}

module appServices 'modules/appservice.bicep' = {
  name: 'appservices-deployment'
  params: {
    baseName: baseName
    env: env
    location: location
    logAnalyticsId: monitoring.outputs.logAnalyticsId
    acrName: acr.outputs.acrName
    keyVaultName: keyVault.outputs.keyVaultName
    appInsightsConnectionString: monitoring.outputs.appInsightsConnectionString
  }
}

// ── Fabric Capacity (conditional) ────────────────────────────────────────────
module fabricCapacity 'modules/fabric_capacity.bicep' = if (!empty(fabricSku)) {
  name: 'fabric-capacity-deployment'
  params: {
    capacityName: resolvedCapacityName
    location: location
    fabricSku: fabricSku
    adminMembers: fabricCapacityAdmins
  }
}

output acrName string = acr.outputs.acrName
output keyVaultName string = keyVault.outputs.keyVaultName
output backendAppName string = appServices.outputs.backendAppName
output frontendAppName string = appServices.outputs.frontendAppName
output logAnalyticsWorkspaceId string = monitoring.outputs.logAnalyticsId
output vnetId string = enablePrivateEndpoints ? networking.outputs.vnetId : ''

// Fabric capacity output — empty string when fabricSku is not set.
// Uses null-conditional ?.  to avoid BCP318 on the conditional module reference.
// The Fabric capacity GUID for workspace assignment is NOT available here;
// it is looked up via the Fabric REST API in the deploy workflow after this
// Bicep deployment completes (GET /v1/capacities, match on displayName).
output fabricCapacityName string = fabricCapacity.?outputs.capacityName ?? ''
