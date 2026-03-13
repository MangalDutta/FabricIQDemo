# AI-Powered Customer 360 Intelligence Platform
### One-Click Deployment on Microsoft Fabric + Azure

> **"We transformed traditional BI dashboards into an AI-powered conversational data platform using Microsoft Fabric."**

---

## One-Click Deploy

> ⚠️ **Important — the Deploy to Azure button requires the repository to be public.**
> The Azure Portal fetches `azuredeploy.json` directly from the raw GitHub URL. If the repository is private, the portal will show a "Template not found" error for all users. Make sure your repo is set to **Public** in GitHub → Settings → General before sharing this button.

| Button | What it deploys | Requirements |
|---|---|---|
| [![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FMangalDutta%2FFabricIQDemo%2Fmaster%2Fazuredeploy.json) | **Full stack** — Azure infra + Fabric resources + Notebooks + Docker apps (~20 min) | Azure subscription with **Contributor** access |
| [![Run on GitHub Actions](https://img.shields.io/badge/▶%20Run%20on%20GitHub%20Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/MangalDutta/FabricIQDemo/actions/workflows/deploy.yml) | **Full stack** — same, with live logs and re-run support | Repo write access + GitHub OIDC configured |

> **Deploy to Azure** → Azure Portal opens → Select **Subscription** + **Resource Group** → Fill parameters → **Review + create** → **Create**.
>
> **Run on GitHub Actions** → Actions page opens → click **Run workflow** → fill 6 inputs → **Run workflow**.
>
> **If the Deploy to Azure button shows "Template not found":** Your GitHub repository is either private or the `azuredeploy.json` file has not been pushed yet. See [Requirements](#requirements-for-the-deploy-to-azure-button) below.

### Requirements for the Deploy to Azure Button

For the **Deploy to Azure** button to work for **anyone** (not just you), three things must be true:

1. **Repository must be Public** — Azure Portal fetches the ARM template directly from `raw.githubusercontent.com`. GitHub returns 404 for private repos, and Azure Portal shows "Template not found". Go to: GitHub → your repo → **Settings** → **General** → scroll to **Danger Zone** → **Change repository visibility** → **Public**.

2. **`azuredeploy.json` must be committed and pushed to `master`** — The button links to the `master` branch of `MangalDutta/FabricIQDemo`. If you ever rename or change the default branch, update the button URL to match.

3. **The person clicking only needs an Azure subscription** — They do **not** need access to your GitHub repository. Azure Portal fetches the public template URL and deploys directly into their own subscription.

> **Alternative — host the template on Azure Blob Storage:** If you need the repo to remain private, upload `azuredeploy.json` to a public Azure Blob Storage container and update the button URL to point to the blob's public URL instead of GitHub.

---

### Required Inputs (6 required, 10 optional)

| Input | Required | Default | Where to find it |
|---|---|---|---|
| `azure_subscription_id` | Yes | — | Azure Portal → Subscriptions |
| `azure_tenant_id` | Yes | — | Azure Portal → Microsoft Entra ID → Overview |
| `azure_client_id` | Yes | — | Entra ID → App registrations → your app → Application (client) ID |
| `resource_group` | Yes | — | Any name, e.g. `rg-customer360-demo` (created if absent) |
| `location` | Yes | `centralindia` | Azure region, e.g. `eastus`, `westeurope` |
| `workspace_name` | Yes | — | Pick any name for your Fabric workspace |
| `fabric_sku` | — | *(blank)* | Set to `F2`–`F2048` or `Trial` to provision a new Fabric capacity |
| `fabric_capacity_id` | — | *(blank)* | Existing capacity GUID (Fabric Admin → Capacity settings) |
| `skip_data_upload` | — | `false` | Set `true` if customer data is already loaded |
| `powerbi_report_url` | — | *(blank)* | Leave blank on first deploy; add the embed URL on re-run |
| `base_name` | — | `cust360` | Prefix for all Azure resource names |
| `environment` | — | `dev` | Environment suffix (`dev`, `test`, `prod`) |
| `lakehouse_name` | — | `Customer360Lakehouse` | Fabric Lakehouse display name |
| `table_name` | — | `Customer360` | Delta table name inside the Lakehouse |
| `dataagent_name` | — | `Customer360Agent` | Fabric Data Agent display name |
| `report_name` | — | `Customer360 Report` | Power BI report display name |

> With default naming inputs, resource names will be: `acr-cust360dev`, `app-cust360-backend-dev`, `app-cust360-frontend-dev`, etc. Change `base_name` and `environment` to customise them.


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
```

---

## What This Is

A fully automated, one-click deployment that delivers a **Customer 360 Conversational AI Platform** built entirely on **Microsoft Fabric** — no Azure AI Foundry required.

Business users chat in plain English with their customer data:
> *"Which customers in Maharashtra are high churn risk?"*
> *"Top 5 by lifetime value in Karnataka."*
> *"Show average revenue by segment."*

The **Fabric Data Agent** converts the question to SQL, queries the Lakehouse, and returns the answer instantly.

---

## Architecture

```
customer360.csv
      │
      ▼
┌──────────────────────────────────────────────────────┐
│                  MICROSOFT FABRIC                     │
│                                                       │
│  Lakehouse (Customer360Lakehouse)                     │
│     └── Customer360 table (Delta)                     │
│              │                                        │
│              ├──► Semantic Model (Manually create AUtomatic Report for Demo purpose with 1-click)        │
│              │         │                              │
│              │         ├──► Power BI Report ─────────────► Embedded in React App
│              │         │                              │
│              │         └──► Ontology                  │
│              │              └── Customer entity        │
│              │                   (bound to table)     │
│              │                        │               │
│              └──────────────► Data Agent ────────────────► Chat API (FastAPI)
│                        (ontology + semantic model)     │
│                        (NL → SQL → Answer)            │
│                                                       │
│  Notebooks (00–04)                                    │
│     └── Step-by-step walkthrough of entire pipeline   │
└──────────────────────────────────────────────────────┘
                     │
                     ▼
          React Chat Interface
          (Customer 360 AI Analytics)
```

**Everything runs through Fabric:** data storage (Lakehouse + OneLake), semantic understanding (Ontology), NL queries (Data Agent), visual analytics (Power BI), governance (RBAC + Managed Identity), and reproducible setup (Notebooks).

---

## What Gets Deployed

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
| Semantic Model | Auto-created from Lakehouse (Direct Lake) |
| Ontology (`Customer360Ontology`) | Customer entity with data bindings |
| Data Agent (`Customer360Agent`) | NL chat over the table (ontology-aware) |
| Power BI Report (`Customer360 Report`) | Auto-created from semantic model |
| 5 Fabric Notebooks | Step-by-step demo notebooks (see below) |

---

## Fabric Notebooks

Five Fabric notebooks are automatically deployed to your workspace as part of the single-click deployment. They provide a step-by-step walkthrough of the entire pipeline and serve as both documentation and a live demo environment.

| Notebook | Purpose |
|---|---|
| `00_setup_and_load_data` | Create Lakehouse, upload CSV to OneLake, load as Delta table |
| `01_create_semantic_model` | Trigger default Semantic Model from Lakehouse, poll until ready |
| `02_create_ontology` | Build Customer entity with property bindings, create Ontology via REST |
| `03_create_data_agent` | Create Data Agent with Semantic Model + Ontology attached, publish |
| `04_query_agent` | Readiness check, 6 demo queries, interactive query cell |

### When to use the notebooks

The single-click deployment handles everything automatically. The notebooks are useful for:

- **Live demos** — walk through each step of the pipeline interactively in the Fabric portal
- **Debugging** — if a CI/CD step fails, run the corresponding notebook to isolate the issue
- **Learning** — each notebook explains the Fabric REST API calls it makes
- **Customisation** — modify the ontology entity, add new data sources, or change agent instructions

### Running the notebooks

1. Open your Fabric workspace in the portal
2. The notebooks appear in the workspace item list (deployed automatically)
3. Attach the `Customer360Lakehouse` as the default Lakehouse
4. Run notebooks in order: `00` → `01` → `02` → `03` → `04`
5. Each notebook auto-detects workspace/lakehouse IDs from the Fabric session

### Pipeline flow

```
Notebook 00              Notebook 01             Notebook 02
Upload CSV               Trigger Semantic        Create Ontology
  → Delta table            Model from LH           → Customer entity
                                                    → Data binding
        │                       │                        │
        └───────────────────────┴────────────────────────┘
                                │
                          Notebook 03
                          Create Data Agent
                            → Semantic Model linked
                            → Ontology attached
                            → Publish
                                │
                          Notebook 04
                          Query the Agent
                            → "How many customers?"
                            → "Top 10 by churn risk"
                            → Custom questions
```

---

## Prerequisites

Before running the workflow you need:

**1. Azure subscription** with an OIDC app (service principal) configured in GitHub Actions. The service principal needs Contributor on the resource group (or subscription) and Contributor on the Fabric capacity.

**2. Fabric F-capacity** (F2 or higher). Get the capacity GUID from Fabric Admin Portal → Capacity settings → your capacity → copy the GUID.

**3. Fabric Tenant Settings** (Fabric Admin Portal → Tenant Settings):

```
Allow service principals to use Fabric APIs
Allow service principals to create workspaces
Users can create Fabric items
Ontology item (preview) — enabled
Data agent (preview) — enabled
Copilot and Azure OpenAI Service — enabled
```

**4. Entra App API Permissions** (for the service principal):

```
Microsoft Fabric API:
  Fabric.Workspace.ReadWrite.All
  Fabric.Lakehouse.ReadWrite.All
```

**5. GitHub OIDC** set up (federated credentials on the Entra app pointing to your repo/branch).

---

## Deploy in 5 Minutes

### Step 1 — Run the GitHub Actions Workflow

> **If you don't see the workflow in the Actions tab:** Go to your repo → **Actions** tab → click **"I understand my workflows, go ahead and enable them"**. The **Quick Deploy Customer360** workflow will then appear in the left sidebar.

Go to your repo → **Actions** → **Quick Deploy Customer360** → **Run workflow**

Fill in the 6 required inputs (`azure_subscription_id`, `azure_tenant_id`, `azure_client_id`, `resource_group`, `location`, `workspace_name`) and optionally `fabric_capacity_id`. Leave the rest as defaults.

### What happens during deployment

```
Step  1: Bicep deploys Azure infra (ACR, App Services, monitoring)
Step  2: Collect deployment outputs (ACR name, MI principal ID, etc.)
Step  3: Build + push backend Docker image to ACR
Step  4: fabric_setup.py runs the full Fabric pipeline:
           Workspace → Lakehouse → CSV upload → Delta table
           → Semantic Model → Ontology (Customer entity + bindings)
           → Data Agent (ontology attached) → Publish
           → Deploy 5 notebooks to workspace
Step  5: Build + push frontend Docker image (with Power BI URL baked in)
Step  6: Configure backend App Service (env vars, Docker image)
Step  7: Configure frontend App Service
Step  8: Verify Fabric Agent state
Step  9: Smoke test (health checks, chat API test)
Step 10: Deployment summary with all URLs and IDs
```

### Step 2 — Verify the Deployment

After the workflow completes (~10 minutes), check:

```
Frontend: https://app-cust360-frontend-dev.azurewebsites.net
Backend:  https://app-cust360-backend-dev.azurewebsites.net/health  → {"status":"healthy"}
```

Try a chat message — the app should respond with real customer data from the Fabric Data Agent.

### Step 3 — Add Power BI (optional, recommended for demo)

The workflow tries to auto-create a report. If it succeeded, the embed URL is already baked into the frontend. Check the **Deployment Summary** step in the Actions log.

If the report wasn't auto-created (or you want a richer report):

1. Open your Fabric workspace URL (shown in the deployment summary)
2. Click the Lakehouse → **New report** in the toolbar
3. Add visuals (bar chart: `State` vs `LifetimeValue`, table: `FullName` + `ChurnRiskScore` + `Segment`, slicer: `State`)
4. Save as `Customer360 Report`
5. Click **File → Embed report → Website or portal** and copy the embed URL
6. Re-run the workflow with `powerbi_report_url` set and `skip_data_upload` = `true`

---

## Manual Quickfix

If you already have Fabric workspace + agent running and see "No response received", set these two env vars directly on the **backend** App Service:

1. Azure Portal → App Services → `app-cust360-backend-dev` → **Configuration** → **Application settings**
2. Set `FABRIC_WORKSPACE_ID` = your workspace GUID, `FABRIC_DATAAGENT_ID` = your agent GUID
3. Click **Save** → **Restart**

---

## Sample Questions for the Demo

```
"Top 5 customers by LifetimeValue in Maharashtra"
"Which customers have ChurnRiskScore above 80?"
"Show average MonthlyRevenue by State for Karnataka and Tamil Nadu"
"Count customers by Segment"
"List Startup customers in Delhi with LifetimeValue above 50000"
"Find Enterprise customers with churn risk above 0.7 and monthly revenue above 1500"
```

---

## 5-Minute Demo Script

1. *"Today our customer data lives in Microsoft Fabric — a unified analytics platform."*
2. *"Instead of building dashboards for every question, we created a Fabric Data Agent with a semantic ontology."*
3. *"The ontology maps a Customer entity with properties like LifetimeValue, ChurnRiskScore, and Segment directly to our Delta table."*
4. → Ask: **"Which customers are high churn risk?"**
5. → Show the instant AI response with customer names and scores
6. → Ask a follow-up: **"Now show only customers from Maharashtra."** *(Fabric Data Agent maintains context)*
7. *"The agent understands intent, generates SQL against the Lakehouse, and explains results."*
8. → Point to the Power BI embed: *"For deep-dive analysis, the embedded report is right here."*
9. → Open the Fabric workspace: *"And here are the 5 notebooks that document every step of the pipeline — from CSV upload to this conversation."*
10. *"Everything is secured via Azure Managed Identity and Fabric RBAC — enterprise-grade, one-click deployment."*

---

## Why This Matters

| Traditional BI | This Solution |
|---|---|
| Static dashboards | Dynamic AI conversations |
| BI team dependency for every query | Self-service for business users |
| Days/weeks for new report | Instant answers |
| Manual SQL/DAX writing | Natural language via ontology-aware agent |
| Siloed BI tools | Unified Fabric platform |
| No reproducible pipeline | Notebooks document every step |

---

## Sample Data Schema

The `Customer360` Delta table (1,000 records):

| Column | Type | Description |
|---|---|---|
| `CustomerId` | string | Unique ID (C0001, C0002, ...) |
| `FullName` | string | Customer name |
| `State` | string | Indian state |
| `City` | string | City |
| `Segment` | string | Enterprise / SMB / Startup / Consumer / Retail |
| `LifetimeValue` | decimal | Total LTV in INR |
| `MonthlyRevenue` | decimal | Monthly revenue in INR |
| `ChurnRiskScore` | decimal | 0.0 (low) to 100.0 (high risk) |
| `LastPurchaseDate` | date | Most recent purchase |

---

## Project Structure

```
FabricCustomer360Accelerator/
├── .github/workflows/
│   └── deploy.yml              # One-click GitHub Actions workflow
├── backend/
│   ├── app.py                  # FastAPI backend (chat + Power BI embed)
│   ├── fabric_client.py        # Fabric Data Agent /query client
│   ├── fabric_agent_client.py  # OpenAI Assistants API client (advanced)
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/
│   ├── src/                    # React + TypeScript chat UI
│   ├── Dockerfile
│   └── package.json
├── infra/
│   ├── main.bicep              # Infrastructure-as-code orchestrator
│   └── modules/                # ACR, App Service, monitoring, networking, Fabric
├── notebooks/
│   ├── 00_setup_and_load_data.ipynb     # Lakehouse + CSV + Delta table
│   ├── 01_create_semantic_model.ipynb   # Trigger + poll semantic model
│   ├── 02_create_ontology.ipynb         # Customer entity + data binding
│   ├── 03_create_data_agent.ipynb       # Agent + ontology + publish
│   └── 04_query_agent.ipynb             # Demo queries + interactive
├── sample-data/
│   └── customer360.csv         # 1,000 customer records
├── scripts/
│   ├── fabric_setup.py         # Full Fabric provisioning (CI/CD entry point)
│   └── smoke_test.py           # Post-deployment validation
├── tests/
│   ├── test_backend.py         # Backend unit tests
│   └── test_fabric_setup.py    # Setup script tests
├── azuredeploy.json            # Compiled ARM template
└── README.md                   # This file
```

---

## Security Model

```
GitHub Actions (OIDC)  →  Azure Login (no stored secrets)
        │
        ▼
Azure App Service (Managed Identity)
        │
        ├──► Fabric Data Agent API  (token: api.fabric.microsoft.com)
        ├──► Fabric REST APIs       (workspace, lakehouse, ontology, notebooks)
        └──► ACR                    (container pull)
```

No passwords stored. No API keys in config files. Production-ready.

---

## Troubleshooting

### "No response received." in the chat
The backend is reachable but the Fabric Data Agent returned nothing. Check: (1) `FABRIC_WORKSPACE_ID` and `FABRIC_DATAAGENT_ID` env vars are set on the backend App Service, (2) the Data Agent has the `Customer360` table linked, (3) the agent is Published (not Draft).

### Backend returns 503
`FABRIC_WORKSPACE_ID` or `FABRIC_DATAAGENT_ID` is empty on the App Service. Set them manually (see Manual Quickfix above) or re-run the workflow.

### Fabric 403 "FeatureNotAvailable"
Workspace is not bound to an F-capacity. Pass a valid `fabric_capacity_id` in the workflow inputs.

### Fabric 403 "InsufficientPrivileges"
The service principal lacks Contributor on the capacity:
```bash
az role assignment create \
  --assignee <YOUR_APP_CLIENT_ID> \
  --role "Contributor" \
  --scope "/subscriptions/<SUB_ID>/providers/Microsoft.Fabric/capacities/<CAPACITY_GUID>"
```

### Notebooks not appearing in workspace
If the notebook upload step was skipped (e.g., the `notebooks/` directory was missing from the checkout), re-run the workflow. The upload is idempotent — existing notebooks are updated in place.

### Ontology creation fails
Ensure the **Ontology item (preview)** tenant setting is enabled in the Fabric Admin Portal. The ontology API requires this preview feature flag.

### OIDC auth fails
Check GitHub → Settings → Secrets and variables → Actions → ensure federated credentials on the Entra app match your repo/branch/environment.

---

## Deployment Checklist

- [ ] Fabric F-capacity GUID obtained
- [ ] Service principal has Contributor on the capacity
- [ ] Fabric tenant settings enabled (service principals, ontology preview, data agent preview)
- [ ] GitHub OIDC configured
- [ ] Workflow ran successfully (no red steps)
- [ ] Backend `/health` returns `{"status":"healthy"}`
- [ ] Chat responds to "Top 5 customers by LifetimeValue"
- [ ] Notebooks appear in the Fabric workspace (5 notebooks)
- [ ] (Optional) Power BI report embedded in right panel



