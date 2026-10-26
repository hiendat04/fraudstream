# CI/CD with Jenkins

Every push is tested. A push to the deploy branch is also built and deployed:
the two web APIs, the KServe model, the training pipeline, the Airflow pipelines,
the RAG pipeline, the stream push job, the MCP servers and the three agents. Secrets
live in Jenkins and in the cluster, never in the repository.

## Architecture

```mermaid
flowchart LR
    DEV(["developer"]) -- "git push" --> GH[("GitHub<br/>every branch")]
    GH -. "scanned every 5 min" .-> CI

    subgraph HOST["Docker Desktop"]
        direction LR

        subgraph JENKINS["Jenkins container · multibranch job 'fraudstream'"]
            direction LR
            CI["<b>CI · every branch</b><br/>11 test suites<br/>in parallel"]
            GATE{"deploy branch<br/>and files<br/>changed?"}
            STOP(["tested only"])
            CD["<b>CD · deploy branch</b><br/>build image<br/>tag = commit"]
            CRED[["credentials<br/>postgres · minio · github · gateway"]]
            CI --> GATE
            GATE -- no --> STOP
            GATE -- yes --> CD
            CRED -. "masked in logs" .-> CD
        end

        subgraph KIND["kind cluster"]
            direction TB
            APIS["inference-api<br/>drift-detection<br/><i>helm upgrade</i>"]
            MODEL["KServe fraud-detection<br/><i>kubectl apply</i>"]
            KFP["Kubeflow Pipelines<br/><i>new pipeline version</i>"]
        end

        subgraph COMPOSE["docker compose"]
            direction TB
            TREE[/"deploy folder<br/>AIRFLOW_PROJECT_DIR"/]
            AF["Airflow<br/>6 DAGs"]
            SP["feast-stream-push<br/>offline + online"]
            TREE --> AF
            TREE --> SP
        end
    end

    CD -- "kind load + helm" --> APIS
    CD -- "kind load + apply" --> MODEL
    CD -- "port-forward + upload" --> KFP
    CD -- "rsync + reserialize" --> TREE
    CD -- "restart" --> SP
    CD -- "build image, copy handbook, trigger run" --> AF

    classDef key fill:#1d4ed8,stroke:#1e3a8a,stroke-width:2px,color:#ffffff
    classDef store fill:#0f766e,stroke:#134e4a,color:#ffffff
    class CI,CD key
    class GH store
```

Jenkins runs in the Compose stack and joins two networks: `kind`, to reach the
cluster, and the Compose network, to reach Airflow and the stores. It builds with
the host's Docker through a mounted socket, so images never leave the machine.

| Deploys | When these change | Check after deploy |
|---|---|---|
| Inference API | `api/src`, `ml/src`, its Dockerfile, the chart, the gateway scripts | answers with `x-app-version` = commit, then passes the [gateway check](24_gateway.md#proof), or is rolled back |
| Drift detection API | `api/src`, `api/reference`, its Dockerfile, the chart, the gateway scripts | answers with `x-app-version` = commit, then passes the gateway check, or is rolled back |
| KServe model server | `serving/src`, its Dockerfile, `k8s/models` | a real payment scores through the API |
| Training pipeline | `ml/src`, `pipelines/src`, `k8s/Dockerfile.ml` | a version named after the commit exists |
| Airflow pipelines | `airflow`, `src`, `configs`, feature store code | six DAGs listed, no import errors |
| RAG pipeline | `rag`, its DAG, the Airflow Dockerfile, the DataHub code | a triggered run of `fraudstream_rag_index` succeeds |
| Stream push job | feature store code, `src` | still running 30 s after restart |

Deploying a pipeline registers it; it does not run it. Airflow DAGs and training
runs are still started by hand. The RAG pipeline is the exception: a deploy that
indexes nothing proves nothing, so the stage also runs it and waits.

## Demo

### CI: a feature branch is only tested

Jenkins found the branch on its own, and all seven suites passed. The deploy
stages did not run, because this is not the deploy branch.

![Feature branch discovered](../../images/ci_cd/01-ci-feature-branch-discovered.png)

![Tests only](../../images/ci_cd/02-ci-feature-branch-tests-only.png)

### BEFORE the merge

Images deployed by hand in earlier phases.

Inference API runs `fraudstream-inference:v6`:

![Inference API before](../../images/ci_cd/03-before-inference-api-v6.png)

Drift detection API runs `fraudstream-drift-detection:v2`:

![Drift API before](../../images/ci_cd/04-before-drift-detection-v2.png)

Model server runs `fraudstream-serving:dev`:

![Model server before](../../images/ci_cd/05-before-model-server-dev.png)

### CD: the merge into the deploy branch

The deploy branch appears as its own job. The stage view reads newest first:
build #3 (top) deployed everything green. Build #2 failed at the training pipeline,
and builds #1 and #2 at the Airflow stage, fixed as described under *Things worth
knowing*.

![Deploy branch discovered](../../images/ci_cd/06-cd-deploy-branch-discovered.png)

![Deploy stage view](../../images/ci_cd/07-cd-deploy-branch-stage-view.png)

### AFTER the merge

Every deployed image now carries the commit it was built from, `b3fabea`:

![Commit](../../images/ci_cd/08-after-commit-b3fabea.png)

Inference API now runs `fraudstream-inference:b3fabea`:

![Inference API after](../../images/ci_cd/09-after-inference-api-b3fabea.png)

Drift detection API now runs `fraudstream-drift-detection:b3fabea`:

![Drift API after](../../images/ci_cd/10-after-drift-detection-b3fabea.png)

Model server now runs `fraudstream-serving:b3fabea`:

![Model server after](../../images/ci_cd/11-after-model-server-b3fabea.png)

The training pipeline gained a version named after the same commit:

![Pipeline version](../../images/ci_cd/12-after-training-pipeline-version-b3fabea.png)

## MCP servers and agents

A push to the deploy branch that touches the MCP servers or an agent deploys them in
dependency order. An agent's prompt reaches production only after the agent, running
that prompt, answers its eval questions.

```mermaid
flowchart LR
    PUSH(["push to the deploy branch"]) --> T["Test<br/>mcp_servers · agents · …"]
    T --> M["<b>MCP servers</b><br/>build image · helm rolling update<br/>rollback on failure · check"]
    M --> CX["<b>Context agent</b>"]
    CX --> RK["<b>Risk agent</b>"]
    RK --> CO["<b>Coordinator agent</b><br/>then the UI check"]

    subgraph DA["cicd/deploy_agent.sh &lt;agent&gt;, in each agent stage"]
        direction TB
        R1["1 · register the prompt<br/>alias candidate"] --> R2["2 · render candidate into the ConfigMap<br/>apply the Agent"]
        R2 --> R3["3 · wait until kagent has rolled<br/>the pods onto the new config"]
        R3 --> R4{"4 · eval questions"}
        R4 -- pass --> R5["5 · candidate becomes production<br/>publish to agentregistry"]
        R4 -- fail --> R6["5 · production prompt and previous Agent back<br/>drop candidate · build fails"]
    end

    MLF[("MLflow<br/>Prompt Registry")]
    R1 & R5 & R6 <--> MLF

    classDef key fill:#1d4ed8,stroke:#1e3a8a,stroke-width:2px,color:#ffffff
    classDef store fill:#0f766e,stroke:#134e4a,color:#ffffff
    class M,CX,RK,CO key
    class MLF store
```

| Stage | Runs when these change | Does | Fails safe by |
|---|---|---|---|
| MCP servers | `mcp_servers`, `k8s/Dockerfile.mcp`, `k8s/mcp`, the chart | builds `fraudstream-mcp:<commit>`, Helm rolling update of both servers, `k8s/mcp/check.sh` | Helm putting the old pods back; the agent stages then do not run |
| Context agent | its prompt and YAML, the shared agent code, anything above | `cicd/deploy_agent.sh context-agent` | the prompt gate below |
| Risk agent | its prompt and YAML, the shared agent code, anything above | `cicd/deploy_agent.sh risk-agent` | the prompt gate below |
| Coordinator agent | its prompt and YAML, either specialist, anything above | `cicd/deploy_agent.sh coordinator-agent`, then the kagent UI through the gateway | the prompt gate below |

### The prompt gate

The prompt lives in git (`agents/prompts/<agent>.md`) and is versioned in MLflow's
Prompt Registry. Two aliases say what each version is:

- `candidate`: the version a deploy is checking right now. It exists only during a deploy.
- `production`: the last version that passed its eval questions. It is what runs.

`cicd/deploy_agent.sh <agent>`:

1. Registers the prompt as a new version if its text changed, and points `candidate` at it.
2. Renders `@candidate` into the agent's ConfigMap, annotated `prompts:/<agent>/<n>`,
   and applies the Agent.
3. Waits until kagent has reconciled the Agent and written the new prompt into the
   agent's config, then for the pods to roll. A specialist pod that still loaded its
   tools from an old address is restarted once.
4. Asks the agent's eval questions (`k8s/agents/check.sh --agent <agent>`).
5. On a pass, moves `production` to the candidate, drops `candidate` and publishes to
   agentregistry. On a fail, renders `@production` back, applies the Agent YAML from
   the last successful build's commit, drops `candidate` and fails the build.

### Demo

**A broken prompt is rolled back.** The context agent's prompt was replaced with one
that answers from memory, and its customer tool was taken out of the Agent. The eval
questions failed, and the deploy put the production prompt (v7) and the Agent from git back:

![Broken prompt rolled back](../../images/ci_cd/13-agent-deploy-rolled-back.png)

While that deploy was checking it, `candidate` pointed at the broken v8 and
`production` stayed on v7:

![Candidate and production](../../images/ci_cd/14-mlflow-prompt-candidate.png)

**CI: the feature branch is only tested.** The four new stages are skipped:

![Feature branch, agent stages skipped](../../images/ci_cd/15-ci-feature-branch-agents-skipped.png)

**BEFORE the merge**, the coordinator runs prompt v1:

![Coordinator before](../../images/ci_cd/16-before-coordinator-production.png)

**CD: the merge.** The branch changed the coordinator's prompt to put its decision
line last. With that prompt, the coordinator dropped the model's threshold from its
payment answer. The gate failed it twice (v2 and v3), and v1 stayed in production
both times. The change was reverted. The revert registered the old text again as
v4, which passed. With **DEPLOY_ALL**, all four stages went green:

![Agent stages](../../images/ci_cd/17-cd-agents-stage-view.png)

**AFTER the merge**, `production` is on v4. v2 and v3 failed, so they hold no alias:

![Coordinator after](../../images/ci_cd/18-after-coordinator-production.png)

### Secrets

| Secret | Lives in | Used by |
|---|---|---|
| The kubeconfig for the kind cluster | the Jenkins volume, from `cicd/jenkins/kubeconfig.sh` | every stage |
| The `gateway` user and password | Jenkins credential `gateway` | the UI check after the coordinator |
| MCP, A2A and LLM keys | Kubernetes Secrets, made once by the install scripts | the agents; `deploy_agent.sh` only copies a specialist's MCP key into `kagent` |

MLflow and agentregistry need no credential: Jenkins reaches MLflow on the Compose
network and agentregistry through a port-forward.

### Limits

- **The platform is installed by hand.** kagent, the gateways and KEDA come from
  `k8s/agents/install.sh` and `k8s/mcp/install.sh`; the pipeline deploys the MCP
  servers and the agents onto them.
- **A run is slow.** One model answers one request at a time, so the agent stages
  take 5 to 11 minutes each.
- **The model can fail a question it passed before.** That fails the build and
  leaves production as it was; run the build again.

## Setup

```bash
# .env (gitignored) holds the secrets Jenkins reads at start-up
JENKINS_ADMIN_PASSWORD=...  GITHUB_USER=...  GITHUB_TOKEN=...
AIRFLOW_PROJECT_DIR=/Users/Admin/MLOps/fraudstream-deploy

docker compose --profile ci up -d jenkins          # http://localhost:18086
./cicd/jenkins/kubeconfig.sh                         # point Jenkins at the kind cluster
./cicd/sync_airflow.sh "$AIRFLOW_PROJECT_DIR"        # fill the deploy folder once
```

The deploy branch is `DEPLOY_BRANCH` in the `Jenkinsfile`. **Build with
Parameters → DEPLOY_ALL** redeploys everything without a code change.

## Things worth knowing

- **Secrets reach Jenkins as files**, through Docker secrets, not environment
  variables, so no build inherits them. The build log shows `POSTGRES_PASSWORD=****`.
- **Never delete the deploy folder while Airflow runs.** Airflow keeps the deleted
  folder mounted and fails with `Operation not permitted: '/opt/airflow/dags'`.
  Recreate its containers with `--force-recreate` to fix it.
- **The model ships inside its image.** The manifest names the model as
  `fraudstream-serving:model-<version>`. The `Model server` stage packages that version
  from MLflow with its card onto the serving code image, deploys
  `model-<version>-<code commit>`, and points MLflow's `production` alias at whatever
  serves all traffic. No pod downloads a model at start.
- **A merge patch drops container settings.** `kubectl patch --type=merge` on the
  InferenceService replaced the container list and lost `MODEL_URI`, so the stage
  applies the manifest with only the image tag changed.
- **The JVM ignores `$HOME`.** Spark's 1.2 GB jar cache is pointed at the Jenkins
  volume with `spark.jars.ivy`; otherwise concurrent builds raced to download it.
- **The RAG stage builds the Airflow image, but a person recreates Airflow.** Recreating
  it would kill running tasks, so the build turns unstable until
  `docker compose --profile orchestration up -d` is run.
- **A change to a Dockerfile's dependencies** for the stream push job still needs
  `docker compose --profile feature-store up -d --build feast-stream-push`.
- **A port-forward in the background must start kubectl itself.** Started through a
  shell function, `kill $!` killed only the subshell, the forward kept its port,
  and the next agent stage could not open it.
- **llama.cpp's prompt cache in RAM defaults to 8 GiB.** Each agent call saved
  another prompt state until the model server passed its 5 GiB limit and was
  OOMKilled mid-answer, which the agents saw as 503s. `--cache-ram 512` caps it.

## Where things are

| What | Where |
|---|---|
| The pipeline | `Jenkinsfile` |
| Deploying one agent through its prompt gate | `cicd/deploy_agent.sh` |
| The prompts and their MLflow aliases | `agents/prompts/`, `agents/src/fraudstream_agents/prompts.py` |
| The eval questions | `agents/src/fraudstream_agents/evals.py`, `k8s/agents/check.sh --agent` |
| Publishing to agentregistry | `k8s/agents/publish.sh` |
| The MCP servers' install and check | `k8s/mcp/install.sh`, `k8s/mcp/check.sh` |
| The gateway check | `cicd/check_gateway.sh` |
