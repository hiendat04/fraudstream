# FraudStream: the ML and LLM Platform

A payment flagged by the fraud detection model is held for a fraud analyst, who must
decide fast why it was flagged and what to do. On top of the data platform
([`docs/edai-1/`](../edai-1/)), this part trains and serves that model, and gives analysts
a copilot: a coordinator agent that asks two sandboxed specialists, which answer from the
same features and model through MCP tools and cite the handbook through RAG. The model
makes every decision; the agents only report and explain it.

![FraudStream LLM platform](../../images/architecture/llm-platform.svg)

| Flow | What moves |
|---|---|
| **Q** question | the analyst's question to the coordinator, then through the A2A gateway to a specialist |
| **L** LLM | every agent's chat completions through the LLM gateway and llm-d to Qwen3 |
| **M** MCP tools | a specialist's tool calls through the MCP gateway to the APIs and stores |
| **R** RAG | the handbook, model cards and feature pages, embedded into pgvector daily |
| **D** delivery | an agent's prompt, deployed only when its eval questions pass |
| **O** observability | metrics, logs and traces, and the dashboards on them |
| **V** security | every key from Vault |

The ML side (features, training, serving, A/B tests) is drawn in the lower half of the
[data and ML platform drawing](../../images/architecture/data-and-ml-platform.svg).

## Contents

| # | Doc | The main idea |
|---|---|---|
| 01 | [LLM inference platform](01_llm_inference_platform.md) | Qwen3 on llama.cpp behind llm-d and agentgateway |
| 02 | [Global model config](02_global_model_config.md) | one model config for every agent |
| 03 | [Agent registry](03_agent_registry.md) | agents and MCP servers published by CI |
| 04 | [RAG pipeline](04_rag_pipeline.md) | text → chunks → embeddings in pgvector, daily in Airflow |
| 05 | [Feature store pipelines](05_feature_store_pipelines.md) | offline to online store, and the stream push job |
| 06 | [Training pipeline](06_training_pipeline.md) | Kubeflow distributed training, versioned in MLflow |
| 07 | [ML inference service](07_ml_inference_service.md) | the model on KServe |
| 08 | [Context API and agent](08_context_api_and_agent.md) | FastAPI, its MCP server, and the context agent |
| 09 | [Risk agent](09_risk_agent.md) | the model's score and the handbook's rules |
| 10 | [MCP gateway](10_mcp_gateway.md) | one entry to the MCP servers, a key and tool policy per agent |
| 11 | [A2A gateway](11_a2a_gateway.md) | keys, allow-list and rate limit between agents |
| 12 | [Coordinator agent](12_coordinator_agent.md) | the analysts' entry point, scaled by KEDA |
| 13 | [Agent fleet](13_agent_fleet.md) | every conversation in a gVisor sandbox, restored in 2 s |
| 14 | [Validation and verification](14_validation_and_verification.md) | coverage, mutation, property tests, load test against an SLA |
| 15 | [CI/CD](15_ci_cd.md) | Jenkins, with a prompt gate for agents |
| 16 | [External routing](16_external_routing.md) | NGINX in front of every UI and API |
| 17 | [Infrastructure as code](17_infrastructure_as_code.md) | Terraform and Ansible on Google Cloud |
| 18 | [Observability](18_observability.md) | API, compute, LLM and agent telemetry |
| 19 | [A/B testing](19_ab_testing.md) | model versions and LLM configs side by side on real traffic |
| 20 | [Security](20_security.md) | every secret in Vault, one login per reader |
| 21 | [Fraud rings in a graph](21_memgraph.md) | Memgraph reads Kafka; the risk agent asks who shares a device |

## Where things are

| What | Where |
|---|---|
| The data platform | [`docs/edai-1/`](../edai-1/) |
| The architecture drawings | [`images/architecture/`](../../images/architecture/) |
| The project overview | [`README.md`](../../README.md) |
