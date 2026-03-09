@description('Base name for resources')
param baseName string

@description('Environment')
param env string

@description('Azure region')
param location string

@description('Private endpoint subnet ID')
param privateEndpointSubnetId string = ''

var shortBase = substring(baseName, 0, min(length(baseName), 4))
var uniqueHash = substring(uniqueString(resourceGroup().id), 0, 8)
var keyVaultName = 'kv-${shortBase}-${env}-${uniqueHash}'

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
    enablePurgeProtection: false
    publicNetworkAccess: privateEndpointSubnetId != '' ? 'Disabled' : 'Enabled'
  }
}

output keyVaultName string = keyVault.name
output keyVaultUri string = keyVault.properties.vaultUri
