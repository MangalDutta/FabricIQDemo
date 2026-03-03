@description('Base name for resources')
param baseName string

@description('Environment')
param env string

@description('Azure region')
param location string

@description('ACR SKU')
param acrSku string = 'Basic'

@description('Private endpoint subnet ID')
param privateEndpointSubnetId string = ''

var shortBase = substring(baseName, 0, min(length(baseName), 4))
var uniqueHash = substring(uniqueString(resourceGroup().id), 0, 6)
var acrName = 'acr${shortBase}${env}${uniqueHash}'

resource acr 'Microsoft.ContainerRegistry/registries@2023-11-01-preview' = {
  name: acrName
  location: location
  sku: {
    name: acrSku
  }
  properties: {
    adminUserEnabled: false
    publicNetworkAccess: privateEndpointSubnetId != '' ? 'Disabled' : 'Enabled'
  }
}

output acrName string = acr.name
output acrLoginServer string = acr.properties.loginServer
