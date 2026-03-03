targetScope = 'resourceGroup'

@description('Base name for all resources')
param baseName string = 'cust360'

@description('Environment (dev, test, prod)')
param env string = 'dev'

@description('Azure region for resources')
param location string = resourceGroup().location

@description('Enable private endpoints')
param enablePrivateEndpoints bool = false

var vnetName = 'vnet-${baseName}-${env}'
var logAnalyticsName = 'log-${baseName}-${env}'

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
  }
}

output acrName string = acr.outputs.acrName
output keyVaultName string = keyVault.outputs.keyVaultName
output backendAppName string = appServices.outputs.backendAppName
output frontendAppName string = appServices.outputs.frontendAppName
output logAnalyticsWorkspaceId string = monitoring.outputs.logAnalyticsId
output vnetId string = enablePrivateEndpoints ? networking.outputs.vnetId : ''
