# 🚀 AI-Powered Customer 360 Intelligence Platform
### One-Click Deployment on Microsoft Fabric + Azure

> **"We transformed traditional BI dashboards into an AI-powered conversational data platform using Microsoft Fabric."**

---

## ⚡ One-Click Deploy

Choose the deployment option that fits your scenario:

| Button | What it deploys | Requirements |
|---|---|---|
| [![🚀 Deploy Customer360](https://img.shields.io/badge/🚀%20Deploy%20Customer360-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://github.com/MangalDutta/FabricCustomer360Accelerator/actions/workflows/deploy.yml) | **Full stack** — Azure infra + Fabric workspace + Docker images + App Service configuration (~10 min) | GitHub OIDC configured (see Prerequisites) |

> **Full stack** button → **"Run workflow"** → Fill in the inputs → **"Run workflow"** again.

### Full Stack — Required Inputs (6 required, 10 optional)

| Input | Required | Default | Where to find it |
|---|---|---|---|
| `azure_subscription_id` | ✅ | — | Azure Portal → Subscriptions |
| `azure_tenant_id` | ✅ | — | Azure Portal → Microsoft Entra ID → Overview |
| `azure_client_id` | ✅ | — | Entra ID → App registrations → your app → Application (client) ID |
| `resource_group` | ✅ | — | Any name, e.g. `rg-customer360-demo` (created if absent) |
| `location` | ✅ | `centralindia` | Azure region, e.g. `eastus`, `westeurope` |
| `workspace_name` | ✅ | — | Pick any name for your Fabric workspace |
| `fabric_sku` | — | *(blank)* | Set to `F2`–`F2048` or `Trial` to provision a new Fabric capacity |
| `fabric_capacity_id` | — | *(blank)* | Existing capacity GUID (Fabric Admin → Capacity settings) |
| `skip_data_upload` | — | `false` | Set `true` if customer data is already loaded |
| `powerbi_report_url` | — | *(blank)* | Leave blank on first deploy; add the embed URL on re-run |
| `base_name` | — | `cust360` | Prefix for all Azure resource names (ACR, App Service, etc.) |
| `environment` | — | `dev` | Environment suffix appended to resource names (`dev`, `test`, `prod`) |
| `lakehouse_name` | — | `Customer360Lakehouse` | Fabric Lakehouse display name to create or reuse |
| `table_name` | — | `Customer360` | Delta table name inside the Lakehouse |
| `dataagent_name` | — | `Customer360Agent` | Fabric Data Agent display name to create or reuse |
| `report_name` | — | `Customer360 Report` | Power BI report display name to create or reuse |

> With default naming inputs, resource names will be: `acr-cust360dev`, `app-cust360-backend-dev`, `app-cust360-frontend-dev`, etc. Change `base_name` and `environment` to customise them.

### Infra Only — Azure Portal Parameters (Deploy to Azure)

Click the **Deploy to Azure** button above and the Azure Portal will open with a custom deployment form. Fill in the following parameters:

| Parameter | Required | Default | Description |
|---|---|---|---|
| **Subscription** | ✅ | — | Select your Azure subscription |
| **Resource Group** | ✅ | — | Select an existing resource group or create a new one |
| `baseName` | — | `cust360` | Prefix for all resource names (ACR, App Service, etc.) |
| `env` | — | `dev` | Environment suffix (e.g. `dev`, `test`, `prod`) |
| `location` | — | Resource group location | Azure region for all resources |
| `enablePrivateEndpoints` | — | `false` | Set to `true` to deploy VNet + private endpoints for ACR |
| `fabricSku` | — | *(blank)* | Set to `F2`–`F2048` or `Trial` to provision a new Fabric capacity. Leave blank to skip. |
| `fabricCapacityName` | — | *(auto-generated)* | Override the auto-generated Fabric capacity name (lowercase alphanumeric, 3-63 chars) |
| `fabricCapacityAdmins` | ⚠️ | `[]` | **Required when `fabricSku` is set** — JSON array of admin UPNs, e.g. `["admin@contoso.com"]` |

> After the infra-only deployment, run the **Full stack** workflow with `skip_data_upload=false` to build and deploy the Docker images and set up the Fabric workspace.

### CLI Deployment (alternative)

You can also deploy the ARM template directly via Azure CLI:

```bash
# Deploy infrastructure only
az deployment group create \
  --resource-group <your-resource-group> \
  --template-file azuredeploy.json \
  --parameters baseName=cust360 env=dev location=centralindia

# Or with a Fabric capacity
az deployment group create \
  --resource-group <your-resource-group> \
  --template-file azuredeploy.json \
  --parameters baseName=cust360 env=dev location=centralindia \
               fabricSku=F2 fabricCapacityAdmins='["admin@contoso.com"]'

# Or using the parameters file
az deployment group create \
  --resource-group <your-resource-group> \
  --template-file azuredeploy.json \
  --parameters @azuredeploy.parameters.json
```

---

## 🎯 What This Is

A fully automated, one-click deployment that delivers a **Customer 360 Conversational AI Platform** built entirely on **Microsoft Fabric** — no Azure AI Foundry required.

Business users chat in plain English with their customer data:
> *"Which customers in Maharashtra are high churn risk?"*
> *"Top 5 by lifetime value in Karnataka."*
> *"Show average revenue by segment."*

The **Fabric Data Agent** converts the question to SQL, queries the Lakehouse, and returns the answer instantly.

---

## 🏗️ Architecture (Fabric-Only)

```
Customer CSV
    │
    ▼
┌─────────────────────────────────────────────┐
│              MICROSOFT FABRIC                │
│                                             │
│  Lakehouse (Delta Table: Customer360)        │
│       │                                     │
│       ├──► Default Semantic Model           │
│       │         │                           │
│       │         └──► Power BI Report ──────────► Embedded in React App
│       │                                     │
│       └──► Fabric Data Agent ──────────────────► Chat API (FastAPI)
│                (NL → SQL → Answer)          │
└─────────────────────────────────────────────┘
                    │
                    ▼
         React Chat Interface
         (Customer 360 AI Analytics)
```

**Everything runs through Fabric:**
- Data storage → **Fabric Lakehouse** (OneLake, Delta tables)
- NL queries → **Fabric Data Agent** (natural language → SQL → results)
- Visual analytics → **Power BI** (semantic model from Lakehouse, embedded in app)
- Governance → **Fabric RBAC**, managed identity, unified access

---

## 📦 What Gets Deployed

### Azure Resources (automated via Bicep)
| Resource | Purpose |
|---|---|
| Container Registry (ACR) | Hosts Docker images |
| App Service Plan (B1 Linux) | Runs both apps |
| Backend App Service | FastAPI → Fabric Data Agent |
| Frontend App Service | React chat + Power BI embed |
| Log Analytics + App Insights | Monitoring |

### Fabric Resources (automated via Python)
| Resource | Purpose |
|---|---|
| Workspace | Bound to your F-capacity |
| Lakehouse (`Customer360Lakehouse`) | Delta table storage |
| Delta Table (`Customer360`) | 1,000 customer records |
| Fabric Data Agent (`Customer360Agent`) | NL chat over the table |
| Default Semantic Model | Auto-created from Lakehouse |
| Power BI Report (`Customer360 Report`) | Auto-created from semantic model |

---

## ✅ Prerequisites

Before running the workflow you need:

**1. Azure subscription** with an OIDC app (service principal) configured in GitHub Actions. The service principal needs:
- `Contributor` on the resource group (or subscription)
- `Contributor` on the Fabric capacity (for workspace binding)

**2. Fabric F-capacity** (F2 or higher). Get the capacity GUID from:
`Fabric Admin Portal → Capacity settings → your capacity → copy the GUID`

**3. Fabric Tenant Settings** (Fabric Admin Portal → Tenant Settings):
```
✅ Allow service principals to use Fabric APIs
✅ Allow service principals to create workspaces
✅ Users can create Fabric items
```

**4. Entra App API Permissions** (for the service principal):
```
Microsoft Fabric API:
  ✅ Fabric.Workspace.ReadWrite.All
  ✅ Fabric.Lakehouse.ReadWrite.All
```

**5. GitHub OIDC** set up (federated credentials on the Entra app pointing to your repo/branch).

---

## 🚀 Deploy in 5 Minutes

### Step 1 — Run the GitHub Actions Workflow

> **If you don't see the workflow in the Actions tab:** Go to your repo → **Actions** tab → click **"I understand my workflows, go ahead and enable them"** to enable GitHub Actions. The **Quick Deploy Customer360** workflow will then appear in the left sidebar.

Go to your repo → **Actions** → **Quick Deploy Customer360** → **Run workflow**

Fill in the inputs:

| Input | Example | Notes |
|---|---|---|
| `azure_subscription_id` | `xxxxxxxx-...` | Your Azure sub ID |
| `azure_tenant_id` | `xxxxxxxx-...` | Your Entra tenant ID |
| `azure_client_id` | `xxxxxxxx-...` | OIDC app client ID |
| `resource_group` | `rg-customer360-demo` | Created if absent |
| `location` | `centralindia` | Azure region |
| `workspace_name` | `fabricagentdemo` | Fabric workspace name |
| `fabric_capacity_id` | `44BF8C5D-...` | F-capacity GUID |
| `powerbi_report_url` | *(leave blank first time)* | See Step 3 |
| `base_name` | `cust360` | Prefix for Azure resource names |
| `environment` | `dev` | Environment suffix (`dev`, `test`, `prod`) |
| `lakehouse_name` | `Customer360Lakehouse` | Fabric Lakehouse name |
| `table_name` | `Customer360` | Delta table name |
| `dataagent_name` | `Customer360Agent` | Fabric Data Agent name |
| `report_name` | `Customer360 Report` | Power BI report name |

> The last 6 inputs (naming/customisation) have sensible defaults — leave them as-is for a standard demo deployment.

### Step 2 — Verify the Deployment

After the workflow completes (~10 minutes), check:

```
Frontend: https://xxxxxxxxxxxxxxxxxx.azurewebsites.net
Backend:  https://xxxxxxxxxxxxxxxxxx.azurewebsites.net/health  → {"status":"healthy"}
```

Try a chat message — the app should respond with real customer data from the Fabric Data Agent.

### Step 3 — Add Power BI (optional, recommended for demo)

The workflow tries to auto-create a report. If it succeeded, the embed URL is already baked into the frontend. Check the **Deployment Summary** step in the Actions log.

If the report wasn't auto-created (or you want a richer report):

1. Open your Fabric workspace URL (shown in the deployment summary)
2. Click the Lakehouse → **New report** in the toolbar
3. Add visuals:
   - Bar chart: `State` vs `LifetimeValue`
   - Table: `FullName`, `ChurnRiskScore`, `Segment`, `MonthlyRevenue`
   - Slicer: `State`
4. Save the report as `Customer360 Report`
5. Click **File → Embed report → Website or portal**
6. Copy the embed URL
7. Re-run the workflow with:
   - `powerbi_report_url` = the copied URL
   - `skip_data_upload` = `true` (data is already loaded)

The frontend will now show the Power BI dashboard on the right side.

---

## 🔧 Manual Quickfix (if you already have Fabric workspace + agent running)

If you want to fix the "No response received" error without a full redeploy, set these two env vars directly in the Azure Portal on the **backend** App Service:

1. Azure Portal → App Services → `app-cust360-backend-dev` → **Configuration** → **Application settings**
2. Add or update:

| Name | Value |
|---|---|
| `FABRIC_WORKSPACE_ID` | Your Fabric workspace GUID |
| `FABRIC_DATAAGENT_ID` | Your Fabric Data Agent item GUID |

3. Click **Save** → **Restart** the App Service

Then re-run the GitHub Actions workflow to rebuild and deploy the Docker images with the latest `fabric_client.py` code (the old Docker image still has the AI Foundry code).

---

## 💬 Sample Questions for the Demo

```
"Top 5 customers by LifetimeValue in Maharashtra"
"Which customers have ChurnRiskScore above 80?"
"Show average MonthlyRevenue by State for Karnataka and Tamil Nadu"
"Count customers by Segment"
"List Startup customers in Delhi with LifetimeValue above 50000"
"Customers in Karnataka with LifetimeValue above 100000"
```

---

## 🎬 5-Minute Demo Script

1. *"Today our customer data lives in Microsoft Fabric — a unified analytics platform."*
2. *"Instead of building a new Power BI dashboard for every question, we created a Fabric Data Agent."*
3. *"Business users simply ask questions in plain English."*
4. → Ask: **"Which customers are high churn risk?"**
5. → Show the instant AI response with customer names and scores
6. → Ask a follow-up: **"Now show only customers from Maharashtra."**  *(Fabric Data Agent maintains context)*
7. *"The agent understands intent, generates SQL against the Lakehouse, and explains results."*
8. → Point to the Power BI embed: *"For deep-dive analysis, the embedded Power BI report is right here."*
9. *"Everything is secured via Azure Managed Identity and Fabric RBAC — enterprise-grade."*

---

## 🏆 Why This Matters

| Traditional BI | This Solution |
|---|---|
| Static dashboards | Dynamic AI conversations |
| BI team dependency for every query | Self-service for business users |
| Days/weeks for new report | Instant answers |
| Manual SQL/DAX writing | Natural language |
| Siloed BI tools | Unified Fabric platform |

---

## 📊 Sample Data Schema

The `Customer360` Delta table (1,000 records):

| Column | Type | Description |
|---|---|---|
| `CustomerId` | string | Unique ID (CUST-XXXX) |
| `FullName` | string | Customer name |
| `State` | string | Indian state |
| `City` | string | City |
| `Segment` | string | Enterprise / SMB / Startup / Consumer |
| `LifetimeValue` | decimal | Total LTV in ₹ |
| `MonthlyRevenue` | decimal | Monthly revenue in ₹ |
| `ChurnRiskScore` | decimal | 0.0 (low) to 100.0 (high risk) |
| `LastPurchaseDate` | date | Most recent purchase |

---

## 🔐 Security Model

```
GitHub Actions (OIDC)  →  Azure Login (no stored secrets)
        │
        ▼
Azure App Service (Managed Identity)
        │
        └──► Fabric Data Agent API  (token: api.fabric.microsoft.com)
        └──► ACR                    (container pull)
```

No passwords stored. No API keys in config files. Production-ready.

---

## 💰 Estimated Monthly Cost

| Resource | Cost |
|---|---|
| App Service B1 (×2) | ~$30/month |
| ACR Basic | ~$5/month |
| Log Analytics | ~$2/month |
| Fabric F2 capacity | ~$262/month (pay-as-you-go, pause when not in use) |
| **Total** | **~$300/month** |

> 💡 Pause the Fabric capacity when not demoing to reduce cost to ~$37/month.

---

## 🔍 Troubleshooting

### "No response received." in the chat
The backend is reachable but the Fabric Data Agent returned nothing. Causes:
1. **New code not deployed** → Re-run the GitHub Actions workflow (the old Docker image has the AI Foundry code)
2. **Missing env vars** → Check `FABRIC_WORKSPACE_ID` and `FABRIC_DATAAGENT_ID` are set on the backend App Service
3. **Data Agent not configured** → In the Fabric portal, open the Data Agent and verify it has the `Customer360` table linked

### Backend returns 503
`FABRIC_WORKSPACE_ID` or `FABRIC_DATAAGENT_ID` is empty on the App Service. Set them manually (see Manual Quickfix above) or re-run the workflow.

### Fabric 403 "FeatureNotAvailable"
Workspace is not bound to an F-capacity. Pass a valid `fabric_capacity_id` in the workflow inputs.

### Fabric 403 "InsufficientPrivileges"
The service principal lacks `Contributor` on the capacity:
```bash
az role assignment create \
  --assignee <YOUR_APP_CLIENT_ID> \
  --role "Contributor" \
  --scope "/subscriptions/<SUB_ID>/providers/Microsoft.Fabric/capacities/<CAPACITY_GUID>"
```

### Docker npm ci fails
Already fixed — the frontend Dockerfile uses `npm install`, not `npm ci`.

### OIDC auth fails
Check GitHub → Settings → Secrets and variables → Actions → ensure federated credentials on the Entra app match your repo/branch/environment.

---

## ✅ Deployment Checklist

- [ ] Fabric F-capacity GUID obtained
- [ ] Service principal has Contributor on the capacity
- [ ] Fabric tenant settings enabled for service principals
- [ ] GitHub OIDC configured
- [ ] Workflow ran successfully (no red steps)
- [ ] Backend `/health` returns `{"status":"healthy"}`
- [ ] Chat responds to "Top 5 customers by LifetimeValue"
- [ ] (Optional) Power BI report embedded in right panel




