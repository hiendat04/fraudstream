# LLM layer

Docs for the analyst copilot built on top of the ML platform: the LLM inference
platform, the RAG pipeline, the context API and MCP servers, the agents and the
gateways between them. Each doc is added here when its part is built.

The fraud model still makes every fraud decision. The LLM layer fetches facts,
explains them and cites its sources.

| Doc | What it covers |
|---|---|
| [29 · LLM inference platform](29_llm_inference_platform.md) | Qwen3 on llama.cpp behind llm-d and agentgateway: keys, limits, PII masking, telemetry, benchmark |
| [30 · RAG data pipeline](30_rag_pipeline.md) | The handbook, model cards and feature dictionary, chunked and embedded daily into Iceberg and Feast's pgvector store, checked and governed |
