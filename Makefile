# hb-ecommerce-lakehouse
# Owner: Hemanjali Buchireddy
#
# A note on demo-up / demo-down.
#
# The usual reason for these targets is to stop per-hour billing. This
# architecture deliberately has NO per-hour resources — that is the whole point
# of choosing Firehose over Kinesis Data Streams ($10.80/mo idle) and CloudWatch
# Logs Insights over OpenSearch ($25.92/mo idle). Firehose, Lambda, Glue, Step
# Functions and Athena all cost exactly $0.00 when nothing is running.
#
# What DOES accrue while idle is small and specific:
#   - CloudWatch alarms      $0.10 per alarm-metric per month
#   - custom freshness metric $0.30 per month
#   - S3 storage              ~$0.023 per GB per month
#
# So `demo-down` targets exactly those: it empties the data buckets and disables
# the alarms. `make destroy` is the real zero — it removes everything.

SHELL       := /bin/bash
PROFILE     ?= hb
REGION      ?= us-east-1
ACCOUNT     ?= 904233128322
TF          := terraform -chdir=terraform
PY          := .venv/bin/python
AWS         := aws --profile $(PROFILE) --region $(REGION)

# Generator defaults. Kept small on purpose: these are the knobs that spend money.
RATE        ?= 200
DURATION    ?= 60
TARGET_GB   ?= 2

.DEFAULT_GOAL := help
.PHONY: help venv guard-account plan apply destroy generate-stream generate-bulk \
        run-pipeline run-pipeline-bench benchmark demo-up demo-down cost status \
        confirm-sns dashboard-env fmt validate clean

## ---------------------------------------------------------------------------
## Help
## ---------------------------------------------------------------------------

help: ## Show available targets
	@echo "hb-ecommerce-lakehouse — Hemanjali Buchireddy"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Cost-sensitive targets are marked [\$$]. Nothing spends money without one of them."

## ---------------------------------------------------------------------------
## Safety
## ---------------------------------------------------------------------------

guard-account: ## Fail loudly if the CLI is pointed at the wrong AWS account
	@acct=$$($(AWS) sts get-caller-identity --query Account --output text 2>/dev/null); \
	if [ "$$acct" != "$(ACCOUNT)" ]; then \
	  echo "REFUSING TO RUN."; \
	  echo "  expected account : $(ACCOUNT)  (Hemanjali's)"; \
	  echo "  resolved account : $$acct"; \
	  echo "  profile          : $(PROFILE)"; \
	  exit 1; \
	fi; \
	echo "account $$acct via profile $(PROFILE) — OK"

venv: ## Create the local Python venv and install boto3
	@test -d .venv || python3 -m venv .venv
	@.venv/bin/pip install -q --upgrade pip
	@.venv/bin/pip install -q -r requirements.txt
	@echo "venv ready: $$(.venv/bin/python -c 'import boto3; print(\"boto3\", boto3.__version__)')"

## ---------------------------------------------------------------------------
## Infrastructure
## ---------------------------------------------------------------------------

fmt: ## Format Terraform
	@$(TF) fmt -recursive

validate: ## Validate Terraform
	@$(TF) validate

plan: guard-account ## Show what would change — creates nothing
	@$(TF) plan

apply: guard-account ## [$] Create the infrastructure (budget is created first)
	@echo "The budget resource is created before every billable resource (enforced by depends_on)."
	@$(TF) apply

destroy: guard-account ## [$ -> 0] Remove EVERYTHING. Monthly cost returns to zero.
	@echo "This deletes all buckets (force_destroy), Glue tables, the state machine and alarms."
	@$(TF) destroy

## ---------------------------------------------------------------------------
## Data generation — nothing below runs automatically
## ---------------------------------------------------------------------------

generate-stream: guard-account venv ## [$] Stream events through Firehose (RATE, DURATION)
	@echo "Streaming ~$(RATE) events/sec for $(DURATION)s through Firehose."
	@echo "Approx cost: \$$$$(echo "scale=4; $(RATE)*$(DURATION)*400/1000000000*0.08" | bc)"
	@$(PY) generator/generate.py --mode stream \
	  --stream-name $$($(TF) output -raw firehose_stream_name) \
	  --rate $(RATE) --duration $(DURATION) \
	  --profile $(PROFILE) --region $(REGION)

generate-bulk: guard-account venv ## [$] Write the benchmark corpus straight to S3 (TARGET_GB)
	@echo "Writing ~$(TARGET_GB) GB to bronze via direct S3 multipart (bypasses Firehose)."
	@$(PY) generator/generate.py --mode bulk \
	  --bucket $$($(TF) output -raw data_bucket) \
	  --target-gb $(TARGET_GB) --seed 42 \
	  --profile $(PROFILE) --region $(REGION)

## ---------------------------------------------------------------------------
## Pipeline
## ---------------------------------------------------------------------------

run-pipeline: guard-account ## [$] Run bronze -> silver -> gold
	@$(AWS) stepfunctions start-execution \
	  --state-machine-arn $$($(TF) output -raw state_machine_arn) \
	  --input '{"build_benchmark": false}' \
	  --query executionArn --output text

run-pipeline-bench: guard-account ## [$] Run the pipeline AND rebuild the three benchmark tables
	@$(AWS) stepfunctions start-execution \
	  --state-machine-arn $$($(TF) output -raw state_machine_arn) \
	  --input '{"build_benchmark": true}' \
	  --query executionArn --output text

benchmark: guard-account venv ## [$] Run the Phase 2 measurements and write BENCHMARK.md
	@$(PY) scripts/run_benchmark.py \
	  --profile $(PROFILE) --region $(REGION) \
	  --workgroup $$($(TF) output -raw benchmark_workgroup) \
	  --database $$($(TF) output -raw glue_database)

## ---------------------------------------------------------------------------
## Demo lifecycle
## ---------------------------------------------------------------------------

demo-up: apply ## [$] Bring the demo up: infra + a short live stream + one pipeline run
	@$(MAKE) generate-stream RATE=200 DURATION=60
	@echo "waiting 90s for Firehose to flush its buffer to bronze..."
	@$(AWS) stepfunctions start-execution \
	  --state-machine-arn $$($(TF) output -raw state_machine_arn) \
	  --input '{"build_benchmark": false}' --query executionArn --output text
	@echo "demo is up. 'make status' to watch, 'make demo-down' to stop the meter."

demo-down: guard-account ## [-> ~0] Empty the data buckets and disable alarms. Infra stays.
	@echo "Emptying data and results buckets (stops S3 storage charges)..."
	@$(AWS) s3 rm s3://$$($(TF) output -raw data_bucket)/ --recursive --only-show-errors || true
	@$(AWS) s3 rm s3://$$($(TF) output -raw results_bucket)/ --recursive --only-show-errors || true
	@echo "Disabling CloudWatch alarm actions..."
	@for a in $$($(AWS) cloudwatch describe-alarms --alarm-name-prefix hb- \
	     --query 'MetricAlarms[].AlarmName' --output text); do \
	  $(AWS) cloudwatch disable-alarm-actions --alarm-names $$a; echo "  disabled $$a"; \
	done
	@echo ""
	@echo "Storage cost stopped. Alarms still exist at \$$0.10/alarm-metric/month."
	@echo "For a true \$$0.00, run: make destroy"

## ---------------------------------------------------------------------------
## Observability
## ---------------------------------------------------------------------------

status: guard-account ## Show pipeline, data and cost status
	@echo "=== last 3 pipeline runs ==="
	@$(AWS) stepfunctions list-executions \
	  --state-machine-arn $$($(TF) output -raw state_machine_arn) --max-results 3 \
	  --query 'executions[].[status,name,startDate]' --output table 2>/dev/null || echo "  none yet"
	@echo "=== object counts by layer ==="
	@for p in bronze quarantine silver gold bench; do \
	  n=$$($(AWS) s3 ls s3://$$($(TF) output -raw data_bucket)/$$p/ --recursive 2>/dev/null | wc -l | tr -d ' '); \
	  echo "  $$p: $$n objects"; \
	done
	@$(MAKE) --no-print-directory cost

cost: ## Show month-to-date spend by service
	@echo "=== month-to-date spend ==="
	@$(AWS) ce get-cost-and-usage \
	  --time-period Start=$$(date -u +%Y-%m-01),End=$$(date -u -v+1d +%Y-%m-%d 2>/dev/null || date -u -d '+1 day' +%Y-%m-%d) \
	  --granularity MONTHLY --metrics UnblendedCost \
	  --group-by Type=DIMENSION,Key=SERVICE \
	  --query 'ResultsByTime[0].Groups[?Metrics.UnblendedCost.Amount!=`0`].[Keys[0],Metrics.UnblendedCost.Amount]' \
	  --output table 2>/dev/null || echo "  Cost Explorer data not yet available"
	@echo "=== budget ==="
	@$(AWS) budgets describe-budgets --account-id $(ACCOUNT) \
	  --query 'Budgets[?BudgetName==`hb-lakehouse-ceiling`].[BudgetName,CalculatedSpend.ActualSpend.Amount,BudgetLimit.Amount]' \
	  --output table 2>/dev/null || true

confirm-sns: guard-account ## Show whether Hemanjali confirmed the alert email
	@$(AWS) sns list-subscriptions-by-topic \
	  --topic-arn $$($(TF) output -raw sns_topic_arn) \
	  --query 'Subscriptions[].[Endpoint,SubscriptionArn]' --output table

dashboard-env: guard-account ## Push read-only AWS creds into Vercel's encrypted env store
	@echo "Sending the dashboard-reader credentials to Vercel. They are never written to disk."
	@$(TF) output -raw dashboard_access_key_id     | vercel env add AWS_ACCESS_KEY_ID production
	@$(TF) output -raw dashboard_secret_access_key | vercel env add AWS_SECRET_ACCESS_KEY production
	@echo "$(REGION)"                              | vercel env add AWS_REGION production
	@$(TF) output -raw dashboard_workgroup         | vercel env add ATHENA_WORKGROUP production
	@$(TF) output -raw glue_database               | vercel env add ATHENA_DATABASE production
	@$(TF) output -raw state_machine_arn           | vercel env add STATE_MACHINE_ARN production

clean: ## Remove local build artifacts
	@rm -rf terraform/.build .venv/__pycache__ **/__pycache__
	@echo "cleaned"
