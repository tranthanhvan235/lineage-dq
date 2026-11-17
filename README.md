# Lineage-DQ

Self-validating data pipelines: LLM agents synthesize and verify data quality (DQ) rules from
the column-level lineage a Spark job already emits (OpenLineage) plus whatever business
metadata exists, and the rules are enforced inline at checkpoints inside the job.

This repository is the artifact for the paper. It contains the prototype used in the paper's
deployment (core library and agents) and a small synthetic example that runs on one machine.
The production pipelines and data of the deployment are proprietary and not included.

## Components

| Path | What it is |
|---|---|
| `src/main/java/org/lineagedq/spark/dq/` | **DqGuard**, the Java checkpoint library embedded in a Spark job: captures each checkpoint's schema, loads the active rules, evaluates them in one aggregate per checkpoint, gates the write, writes reports. |
| `dq-agents/` | Python agents (LangChain, any OpenAI-compatible endpoint): **Gen Rules** (synthesizes rules from lineage + metadata), **Verify** (deterministic checks + LLM review), **Analysis** (explains violations), and the **Orchestrator** that drives the Gen → Verify → regenerate loop. |
| `dq-agents/skills/` | The prompts (skills) of the agents. |
| `schema/` | Schema of the DQ store tables (rules, reports, history). |
| `python/dqguard.py` | Python binding of DqGuard for PySpark jobs. |
| `examples/orders/` | Synthetic example pipeline (4 checkpoints) with business metadata. |

## Requirements

- Java 17, Maven 3.8+
- Python 3.10+ with PySpark 4.1 (`pip install pyspark==4.1.2`)
- For rule generation: an OpenAI-compatible LLM endpoint (`LLM_BASE_URL`, `LLM_API_KEY`, `LLM_MODEL`)

## Quick start

```bash
mvn -DskipTests package                       # -> target/lineage-dq-1.0.0.jar
pip install -r dq-agents/requirements.txt
cp dq-agents/.env.example dq-agents/.env      # fill in LLM_BASE_URL, LLM_API_KEY, LLM_MODEL

# 1) DqGuard only (no LLM): captures checkpoints, lineage and the stage manifest
DQ_AGENT_CHECKPOINTS=false bash examples/orders/run.sh

# 2) With the agents: rules are generated, verified and enforced at the checkpoints
bash examples/orders/run.sh
```

Everything is written under `./work/`: the DQ store in `work/store/dq/` (stage manifests,
rules, reports), the OpenLineage events in `work/lineage/`, the job output in `work/out/`.

## Using DqGuard in a job

```python
from dqguard import DqGuardPy

with DqGuardPy(spark, "warehouse/shop.db/customer_monthly") as dq:
    dq.check("src_orders", orders)          # one call per checkpoint
    ...
    dq.before_write()                       # gate: rules are evaluated, the write may be held
    final.write.parquet(out)
    dq.after_write()                        # post-write analysis and report
```

The dataset name must end with the path the job writes to, so that DqGuard can match it with
the OpenLineage output dataset. Configuration is read from `DQ_*` environment variables
(`src/main/java/org/lineagedq/spark/dq/DqConfig.java`, `dq-agents/dq_env.py`).

## License

To be added after review.
