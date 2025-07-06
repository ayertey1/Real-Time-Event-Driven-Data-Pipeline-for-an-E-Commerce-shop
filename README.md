# E-commerce Data Pipeline

This project implements a **real-time event-driven ETL pipeline** for an e-commerce platform using AWS. The pipeline ingests CSV files, validates and transforms them, and computes KPIs for analytics and reporting.

---

## Project Structure

```
.
├── Dockerfile.transformation          # For transformation container (Spark)
├── Dockerfile.validation              # For validation container (Python)
├── src\
|     ├── validate.py                  # Validation script for schema checks and referential integrity
|     ├── spark_transform.py           # Spark transformation script for KPI generation
|     └── logger_utils.py              # Utility for structured logs to S3
|                   
├── .github/
│   └── workflows/
│       ├── build-validation-dev.yml
│       ├── deploy-validation-prod.yml
│       ├── build-transform-dev.yml
│       └── deploy-transform-prod.yml
└── README.md

```

---

## Main Components

### 1. **S3 Bucket**

**Name:** `ecommence-datastore`

- **Folders:**
  - `staging/` – raw CSVs uploaded by users
  - `raw/` – validated CSVs ready for processing
  - `rejected/` – invalid files with schema errors
  - `logs/` – structured JSON logs

---

### 2. **Validation Service**

- **Script:** `validate.py`
- **Packaged as:** ECS Fargate Task (`validation-task`)
- **What it does:**
  - Polls S3 `staging/` for new files.
  - Validates columns against expected schemas:
    - `products.csv`
    - `orders.csv`
    - `order_items.csv`
  - Checks referential integrity:
    - `order_items.order_id` exists in `orders.order_id`
    - `order_items.product_id` exists in `products.id`
  - Moves files to `raw/` or `rejected/`
  - Generates logs in S3 `logs/validation/`
  - Writes a `validation/result.json` marker indicating success.

---

### 3. **Transformation Service**

- **Script:** `spark_transform.py`
- **Packaged as:** ECS Fargate Task (`transformation-task`)
- **Dependencies:** PySpark
- **What it does:**
  - Reads CSVs from `raw/`
  - Computes KPIs (Key Performance Indicators):
    - **Genre-level KPIs:** per category sales and revenue
    - **Hourly KPIs:** time-series metrics
  - Saves outputs to S3 or downstream services.

---

### 4. **AWS Step Functions**

**Orchestration:**
- Runs the validation ECS task.
- Waits for completion.
- If no valid files are detected, ends gracefully.
- If valid files exist, waits 30 seconds before starting transformation.
- Catches and logs any errors.

**Sample Workflow:**
```
Run Validation Task -> Check Validation Outcome -> (No Valid Files) Succeed
-> (Valid Files) Wait 30s -> Run Transformation Task

```

---

### 5. **GitHub Actions**

**Branching Strategy:**
- `devLab6`: Development
- `prodLab6`: Production

**Workflows:**
- `build-validation-dev.yml`: Build validation image (no push)
- `deploy-validation-prod.yml`: Build & push validation image to ECR
- `build-transform-dev.yml`: Build transformation image (no push)
- `deploy-transform-prod.yml`: Build & push transformation image to ECR

---

## Deployment Guide

### 1. **Prepare ECR Repositories**

Create two repositories:

- `validation`
- `transformation`

```bash
aws ecr create-repository --repository-name validation
aws ecr create-repository --repository-name transformation
```

---

### 2. **Set up GitHub Secrets**

In your repository **Settings > Secrets > Actions**, add:

* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`

---

### 3. **Deploy Images**

Push to `prodLab6` to trigger:

* Validation image build and push
* Transformation image build and push

---

### 4. **Configure ECS Task Definitions**

Make sure each task definition has:

* Proper IAM role (`ecsTaskExecutionRole`)
* VPC subnets and security groups
* Enough CPU & memory

---

### 5. **Create Step Function**

Use the provided JSON definition with:

* `validation-result.json` S3 lookup Lambda
* Clear `Choice` state to branch execution

---

## KPI Outputs

Your pipeline computes:

* **Category Revenue**
* **Hourly Revenue**
* **Number of Orders per Hour**
* **Top Brands**

KPIs are saved in S3 in `kpi/` or your analytics store.

---

## User Guide

### Uploading Files

1. Drop `.csv` files into `staging/products/`, `staging/orders/`, or `staging/order_items/`.
2. The Lambda event triggers Step Functions.
3. Validation either moves files or rejects them.
4. Transformation generates KPIs automatically.

---

### Typical Outcomes

* **All files valid:**

  * Files moved to `raw/`
  * Transformation runs
  * KPIs generated

* **Some files invalid:**

  * Invalid files moved to `rejected/`
  * Valid files processed

* **No files detected:**

  * Workflow ends gracefully, no transformation.

---

## Troubleshooting

| Issue                                | Resolution                                                         |
| ------------------------------------ | ------------------------------------------------------------------ |
| `profile file cannot be null`        | Ensure you removed `ProfileCredentialsProvider` and use task role. |
| ECS task exits with code 1           | Check CloudWatch logs for Python errors.                           |
| Step Function fails invoking Lambda  | Confirm IAM permissions for `lambda:InvokeFunction`.               |
| Docker build fails in GitHub Actions | Check Dockerfile syntax and ECR permissions.                       |
| No KPIs generated                    | Verify files were moved to `raw/` and contained correct columns.   |

---

## User Manual

1. **Trigger:** Upload files to `staging/`.
2. **Validation:**

   * Schema checks
   * Referential integrity
   * Logs to S3
3. **Transformation:**

   * PySpark aggregates
   * Writes KPIs
4. **Monitoring:**

   * CloudWatch logs
   * S3 logs folder
   * Step Function execution history

---

## Best Practices

* Use consistent file naming.
* Clean `raw/` after transformation.
* Rotate ECR credentials regularly.
* Tag S3 logs with lifecycle policies.

---

## Tips

* You can test validation locally by running:

  ```
  python validate.py
  ```
* For transformations:

  ```
  docker run --rm transformation:latest
  ```
* To reprocess old data, re-upload to `staging/`.

---

