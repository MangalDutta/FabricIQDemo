# Fabric Customer360 Accelerator 🚀

**Fully working one-click deployment to Azure + Fabric** with **all known issues resolved**.

## 🎯 What This Deploys

```
Azure (Automated):
├── Container Registry (ACR)
├── App Service Plan (B1 Linux)
├── Backend API (FastAPI + Fabric REST)
├── Frontend UI (React + Chat)
├── Key Vault (secrets)
└── Monitoring (Log Analytics)

Fabric (Automated):
├── Workspace (bound to your capacity)
├── Lakehouse (Customer360Lakehouse)
└── Sample data loaded
```

## ✅ Deployment Status

| Component | Status | Notes |
|-----------|--------|-------|
| Azure Infrastructure | ✅ Complete | Bicep + GitHub Actions |
| Docker Images | ✅ Complete | Backend + Frontend |
| App Services | ✅ Running | Containerized apps |
| Fabric Workspace | ✅ Complete | **Auto-bound to your capacity** |
| Lakehouse | ✅ Complete | **Auto-created** |
| Sample Data | ⚠️ Manual | Upload CSV (30 seconds) |

## 🚀 Quick Start (5 minutes)

### Prerequisites
- Azure subscription
- **Fabric F-capacity** (F2/F64) with **Contributor role** assigned to your OIDC app
- GitHub repo with OIDC configured

### 1. Deploy
```
Actions → Quick Deploy Customer360 → Run workflow
```
**Key inputs:**
```
Fabric capacityId: 44BF8C5D-61B3-4227-9AA8-98D8E5B75C6B  # ← YOUR CAPACITY ID
Workspace name: fabricagentdemo
```

### 2. Test
```
Frontend: https://app-cust360-frontend-dev.azurewebsites.net
Backend: https://app-cust360-backend-dev.azurewebsites.net/health
Query: "Top 5 customers by LifetimeValue"
```

## 🛠️ All Issues Fixed (Proactively Documented)

| Issue | Symptoms | Root Cause | Fix Applied |
|-------|----------|------------|-------------|
| **Resource group not created** | `az deployment group create` fails | RG must exist first | Combined `az group create + bicep` |
| **Key Vault name too long** | 26 chars → 24 max | Azure naming limits | Shortened to `kv-cust-dev-...` |
| **Frontend Docker fail** | `npm ci` fails | No `package-lock.json` | Changed to `npm install` |
| **Fabric 401 Unauthorized** | Auth fails | Service principal scopes | Added `Fabric.Workspace.ReadWrite.All` |
| **403 FeatureNotAvailable (Lakehouse)** | Cannot create lakehouse | Workspace needs F-capacity | **Auto-bind workspace to capacityId** |
| **403 InsufficientPrivileges (assignToCapacity)** | Cannot re-bind workspace | SP lacks capacity RBAC | **Docs: Add Contributor role to SP** |

## 🔧 Service Principal Permissions Required

Your OIDC app (`azure_client_id`) needs **these Azure RBAC roles**:

### On Fabric Capacity (`44BF8C5D-61B3-4227-9AA8-98D8E5B75C6B`):
```
Azure CLI:
az role assignment create --assignee <YOUR_APP_ID> --role "Contributor" --scope "/subscriptions/.../microsoft.fabric/capacities/44BF8C5D-61B3-4227-9AA8-98D8E5B75C6B"
```

### Fabric API Scopes (Entra App → API permissions):
```
Fabric.Workspace.ReadWrite.All
Fabric.Lakehouse.ReadWrite.All
```

### Fabric Tenant Settings (Admin Portal):
```
✅ Allow service principals to use Fabric APIs
✅ Allow service principals to create workspaces
```

## 📋 Architecture & Flow

```
1. GitHub Actions (OIDC)
   ↓ Azure Login
2. Bicep → Azure Resources (ACR, App Services)
   ↓ Docker builds
3. Fabric Setup (Python + Fabric REST API):
   ├── Find/create workspace fabricagentdemo
   ├── Bind to capacityId: 44BF8C5D-61B3-4227-9AA8-98D8E5B75C6B
   └── Create lakehouse Customer360Lakehouse
4. Apps deployed → Query lakehouse via Fabric REST
```

## 🎯 Test Your Deployment

**Expected URLs** (from deployment summary):
```
Frontend: https://app-cust360-frontend-dev.azurewebsites.net
Backend:  https://app-cust360-backend-dev.azurewebsites.net/health ✅ 200
```

**Sample queries:**
```
"Top 5 customers by LifetimeValue"
"Customers from Karnataka"
"Average LifetimeValue by State"
```

## 📊 Costs

```
App Service B1: ~$15/month
ACR Basic: ~$5/month
Fabric F2: ~$262/month (pay-as-you-go)
TOTAL: ~$280/month
```

## 🔍 Troubleshooting

### Fabric 403 "FeatureNotAvailable"
```
→ Workspace not on F-capacity
FIX: Set fabric_capacity_id = 44BF8C5D-61B3-4227-9AA8-98D8E5B75C6B
```

### Fabric 403 "InsufficientPrivileges"
```
→ SP lacks Contributor on capacity
FIX: az role assignment create --assignee <APP_ID> --role "Contributor" --scope <CAPACITY_ID>
```

### Docker npm ci fails
```
→ No package-lock.json
FIX: Already fixed (npm install)
```

### OIDC auth fails
```
→ Wrong client_id/tenant_id
FIX: Check GitHub → Settings → Secrets → Federated credentials
```

## 🎉 Success Checklist

- [ ] Workflow completes ✅ (no 403s)
- [ ] Backend `/health` returns 200 ✅
- [ ] Frontend loads ✅
- [ ] Workspace shows capacity `44BF8C5D-61B3-4227-9AA8-98D8E5B75C6B` ✅
- [ ] Lakehouse `Customer360Lakehouse` exists ✅
- [ ] Query "Top 5 customers" works ✅

---

