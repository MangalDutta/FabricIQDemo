@description('Name for the Fabric capacity (lowercase alphanumeric and hyphens, 3-63 chars, globally unique per region)')
param capacityName string

@description('Azure region for the Fabric capacity')
param location string

@description('''
Fabric capacity SKU.
  F2–F2048  : paid Fabric compute SKUs (F2, F4, F8, F16, F32, F64, F128, F256, F512, F1024, F2048)
  Trial     : free 60-day trial (maps to F1 / Trial tier, one per tenant)
''')
@allowed(['F2', 'F4', 'F8', 'F16', 'F32', 'F64', 'F128', 'F256', 'F512', 'F1024', 'F2048', 'Trial'])
param fabricSku string

@description('UPNs / email addresses of Fabric capacity administrators (at least one required)')
param adminMembers array

// ── Derive SKU name and tier from the user-friendly input ────────────────────
// Trial maps to sku.name = 'F1' with sku.tier = 'Trial'.
// All paid SKUs use sku.tier = 'Fabric'.
var skuName = fabricSku == 'Trial' ? 'F1' : fabricSku
var skuTier = fabricSku == 'Trial' ? 'Trial' : 'Fabric'

// ── Fabric Capacity resource ─────────────────────────────────────────────────
resource fabricCapacity 'Microsoft.Fabric/capacities@2023-11-01' = {
  name: capacityName
  location: location
  sku: {
    name: skuName
    tier: skuTier
  }
  properties: {
    administration: {
      members: adminMembers
    }
  }
}

// The Fabric capacity GUID (used by the Fabric REST API to assign a workspace)
// is NOT exposed as an ARM resource property — it must be retrieved after
// deployment via GET https://api.fabric.microsoft.com/v1/capacities and
// matching on displayName == capacityName.
output capacityName string = fabricCapacity.name
output capacityAzureResourceId string = fabricCapacity.id
