# CI/CD with Jenkins

Every push is tested. A push to the deploy branch is also built and deployed:
the two web APIs, the KServe model, the training pipeline, the Airflow pipelines
and the stream push job. Secrets live in Jenkins, never in the repository.

## Architecture

```mermaid
flowchart LR
    DEV(["developer"]) -- "git push" --> GH[("GitHub<br/>every branch")]
    GH -. "scanned every 5 min" .-> CI

    subgraph HOST["Mac · Docker Desktop"]
        direction LR

        subgraph JENKINS["Jenkins container · multibranch job 'fraudstream'"]
            direction LR
            CI["<b>CI · every branch</b><br/>7 test suites<br/>in parallel"]
            GATE{"deploy branch<br/>and files<br/>changed?"}
            STOP(["tested only"])
            CD["<b>CD · deploy branch</b><br/>build image<br/>tag = commit"]
            CRED[["credentials<br/>postgres · minio · github"]]
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
            AF["Airflow<br/>4 DAGs"]
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
```

Jenkins runs in the Compose stack and joins two networks: `kind`, to reach the
cluster, and the Compose network, to reach Airflow and the stores. It builds with
the host's Docker through a mounted socket, so images never leave the machine.

| Deploys | When these change | Check after deploy |
|---|---|---|
| Inference API | `api/src`, `ml/src`, its Dockerfile, the chart | answers with `x-app-version` = commit |
| Drift detection API | `api/src`, `api/reference`, its Dockerfile, the chart | answers with `x-app-version` = commit |
| KServe model server | `serving/src`, its Dockerfile, `k8s/models` | a real payment scores through the API |
| Training pipeline | `ml/src`, `pipelines/src`, `k8s/Dockerfile.ml` | a version named after the commit exists |
| Airflow pipelines | `airflow`, `src`, `configs`, feature store code | four DAGs listed, no import errors |
| Stream push job | feature store code, `src` | still running 30 s after restart |

Deploying a pipeline registers it; it does not run it. Airflow DAGs and training
runs are still started by hand.

## Demo

### CI: a feature branch is only tested

Jenkins found the branch on its own, and all seven suites passed. The deploy
stages did not run, because this is not the deploy branch.

![Feature branch discovered](../images/ci_cd/01-ci-feature-branch-discovered.png)

![Tests only](../images/ci_cd/02-ci-feature-branch-tests-only.png)

### BEFORE the merge

Images deployed by hand in earlier phases.

Inference API runs `fraudstream-inference:v6`:

![Inference API before](../images/ci_cd/03-before-inference-api-v6.png)

Drift detection API runs `fraudstream-drift-detection:v2`:

![Drift API before](../images/ci_cd/04-before-drift-detection-v2.png)

Model server runs `fraudstream-serving:dev`:

![Model server before](../images/ci_cd/05-before-model-server-dev.png)

### CD: the merge into the deploy branch

The deploy branch appears as its own job. The stage view reads newest first:
build #3 (top) deployed everything green. Build #2 failed at the training pipeline,
and builds #1 and #2 at the Airflow stage, fixed as described under *Things worth
knowing*.

![Deploy branch discovered](../images/ci_cd/06-cd-deploy-branch-discovered.png)

![Deploy stage view](../images/ci_cd/07-cd-deploy-branch-stage-view.png)

### AFTER the merge

Every deployed image now carries the commit it was built from, `b3fabea`:

![Commit](../images/ci_cd/08-after-commit-b3fabea.png)

Inference API now runs `fraudstream-inference:b3fabea`:

![Inference API after](../images/ci_cd/09-after-inference-api-b3fabea.png)

Drift detection API now runs `fraudstream-drift-detection:b3fabea`:

![Drift API after](../images/ci_cd/10-after-drift-detection-b3fabea.png)

Model server now runs `fraudstream-serving:b3fabea`:

![Model server after](../images/ci_cd/11-after-model-server-b3fabea.png)

The training pipeline gained a version named after the same commit:

![Pipeline version](../images/ci_cd/12-after-training-pipeline-version-b3fabea.png)

## Setup

```bash
# .env (gitignored) holds the secrets Jenkins reads at start-up
JENKINS_ADMIN_PASSWORD=...  GITHUB_USER=...  GITHUB_TOKEN=...
AIRFLOW_PROJECT_DIR=/Users/Admin/MLOps/fraudstream-deploy

docker compose --profile ci up -d jenkins          # http://localhost:18086
./ci/jenkins/kubeconfig.sh                         # point Jenkins at the kind cluster
./ci/sync_airflow.sh "$AIRFLOW_PROJECT_DIR"        # fill the deploy folder once
```

The deploy branch is `DEPLOY_BRANCH` in the `Jenkinsfile`. **Build with
Parameters → DEPLOY_ALL** redeploys everything without a code change.

## Things worth knowing

- **Secrets reach Jenkins as files**, through Docker secrets, not environment
  variables, so no build inherits them. The build log shows `POSTGRES_PASSWORD=****`.
- **Never delete the deploy folder while Airflow runs.** Airflow keeps the deleted
  folder mounted and fails with `Operation not permitted: '/opt/airflow/dags'`.
  Recreate its containers with `--force-recreate` to fix it.
- **A merge patch drops container settings.** `kubectl patch --type=merge` on the
  InferenceService replaced the container list and lost `MODEL_URI`, so the stage
  applies the manifest with only the image tag changed.
- **The JVM ignores `$HOME`.** Spark's 1.2 GB jar cache is pointed at the Jenkins
  volume with `spark.jars.ivy`; otherwise concurrent builds raced to download it.
- **A change to a Dockerfile's dependencies** for the stream push job still needs
  `docker compose --profile feature-store up -d --build feast-stream-push`.
