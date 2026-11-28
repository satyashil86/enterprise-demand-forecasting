# Azure integration (final phase): what changes when moving from Free Edition to Azure Databricks

The notebooks, the Bronze/Silver/Gold logic, the model selection and the forecasting stay **unchanged**. Only the
platform underneath changes: storage, identity, secrets, environments and deployment.

## 1. Storage: ADLS Gen2 governed by Unity Catalog
1. Create a storage account with **hierarchical namespace** (ADLS Gen2) and containers `landing` and `lake`.
2. Create an **Access Connector for Azure Databricks** (this gives Databricks a managed identity, so no keys are needed).
3. Give the connector's managed identity the role **Storage Blob Data Contributor** on the storage account.
4. In Databricks (SQL, as a metastore admin):
```sql
CREATE STORAGE CREDENTIAL dp_mi_cred WITH AZURE_MANAGED_IDENTITY
  (ACCESS_CONNECTOR_ID = '/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Databricks/accessConnectors/<name>');
CREATE EXTERNAL LOCATION dp_landing URL 'abfss://landing@<account>.dfs.core.windows.net/' WITH (STORAGE CREDENTIAL dp_mi_cred);
CREATE EXTERNAL LOCATION dp_lake    URL 'abfss://lake@<account>.dfs.core.windows.net/'    WITH (STORAGE CREDENTIAL dp_mi_cred);
CREATE CATALOG demand_planning_prod MANAGED LOCATION 'abfss://lake@<account>.dfs.core.windows.net/demand_planning_prod';
CREATE EXTERNAL VOLUME demand_planning_prod.raw.source_files LOCATION 'abfss://landing@<account>.dfs.core.windows.net/';
```
5. The SAP/POS extracts land in the `landing` container. Auto Loader reads the volume path exactly as before.

## 2. Secrets: Azure Key Vault
- Create a Key Vault-backed secret scope (`demand_planning_prod`) that points to the vault.
- Store `nixtla_api_key` in Key Vault. Notebooks keep using `dbutils.secrets.get("demand_planning_prod", "nixtla_api_key")`.

## 3. Identity and environments
- Separate workspaces for **dev** and **prod**, attached to the same Unity Catalog metastore. Catalogs:
  `demand_planning` (dev), `demand_planning_prod` (prod).
- Users and groups come from **Microsoft Entra ID** through SCIM provisioning (account level), and the same 5 groups get the
  GRANTs from notebook 11.
- Production jobs run as a **service principal** (uncomment `run_as` in `databricks.yml`). Nobody's personal account owns prod.

## 4. CI/CD
- GitHub Actions (`.github/workflows/deploy.yml`) or Azure DevOps runs `databricks bundle validate` and `deploy`.
- Authentication with an Entra ID service principal (OAuth). Secrets live in GitHub/Azure DevOps, never in code.
- Prod deploys need an approval (GitHub "production" environment with a required reviewer).

## 5. Monitoring
- Send Databricks diagnostic logs (jobs, clusters, Unity Catalog, accounts) to a **Log Analytics workspace** with
  Azure Monitor diagnostic settings, and create alerts for failed job runs.
- Keep `monitoring.health_checks` (notebook 10) for data and model health. A Databricks SQL alert on
  `status = 'ALERT'` can email the team.

## 6. Free Edition vs Azure Databricks
| Capability | Free Edition | Azure Databricks |
|---|---|---|
| Compute | serverless only, quotas | serverless + classic clusters, pools, GPUs |
| Storage | Databricks-managed default storage | your ADLS Gen2 via storage credentials and external locations |
| Workspaces | one | dev / test / prod workspaces |
| Identity | workspace users and groups | Entra ID SCIM, service principals, account groups |
| Secrets | Databricks secret scopes | Key Vault-backed scopes |
| Networking | public | VNet injection, Private Link, IP access lists |
| System tables / audit | limited | full audit, lineage and billing system tables |
| SLA / support | none | enterprise SLA |
| Outbound API calls (TimeGPT) | restricted | allowed, controllable via network policies |
| Cost | free | pay per DBU + Azure resources |
