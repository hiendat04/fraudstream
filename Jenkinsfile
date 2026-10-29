// Globals, not def: the when expressions run in another scope and could not read a local.
MCP_PATHS = 'mcp_servers/src mcp_servers/pyproject.toml mcp_servers/uv.lock k8s/Dockerfile.mcp k8s/mcp k8s/charts'
AGENT_PATHS = 'agents/src agents/tools agents/pyproject.toml agents/uv.lock k8s/agents/registry.yaml k8s/agents/check.sh k8s/agents/publish.sh cicd/deploy_agent.sh'
CONTEXT_AGENT_PATHS = 'agents/prompts/context-agent.md k8s/agents/context-agent.yaml'
RISK_AGENT_PATHS = 'agents/prompts/risk-agent.md k8s/agents/risk-agent.yaml'

pipeline {
  agent any

  options {
    timestamps()
    timeout(time: 90, unit: 'MINUTES')
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  }

  parameters {
    booleanParam(
      name: 'DEPLOY_ALL',
      defaultValue: false,
      description: 'Deploy every service, even if its files did not change. Only acts on the deploy branch.'
    )
  }

  environment {
    TAG = "${env.GIT_COMMIT.take(7)}"
    UV_CACHE_DIR = '/var/jenkins_home/uv-cache'
    PYSPARK_SUBMIT_ARGS = '--conf spark.jars.ivy=/var/jenkins_home/.ivy2.5.2 pyspark-shell'
    DEPLOY_BRANCH = 'feature/e2e-fraudstream-platform'
  }

  stages {
    stage('Test') {
      parallel {
        stage('root')          { steps { sh './cicd/test.sh root' } }
        stage('ml')            { steps { sh './cicd/test.sh ml' } }
        stage('pipelines')     { steps { sh './cicd/test.sh pipelines' } }
        stage('feature_store') { steps { sh './cicd/test.sh feature_store' } }
        stage('datahub')       { steps { sh './cicd/test.sh datahub' } }
        stage('api')           { steps { sh './cicd/test.sh api' } }
        stage('serving')       { steps { sh './cicd/test.sh serving' } }
        stage('monitoring')    { steps { sh './cicd/test.sh monitoring' } }
        stage('rag')           { steps { sh './cicd/test.sh rag' } }
        stage('mcp_servers')   { steps { sh './cicd/test.sh mcp_servers' } }
        stage('agents')        { steps { sh './cicd/test.sh agents' } }
      }
    }

    stage('Inference API') {
      when { expression { deploying('api/src ml/src/fraudstream_ml k8s/Dockerfile.inference k8s/apis/inference-api.yaml k8s/apis/secrets.yaml k8s/charts k8s/gateway cicd/gateway.sh cicd/check_gateway.sh') } }
      steps {
        sh '''
          docker build -f k8s/Dockerfile.inference -t "fraudstream-inference:$TAG" .
          kind load docker-image "fraudstream-inference:$TAG" --name fraudstream
          kubectl create namespace fraudstream-apis --dry-run=client -o yaml | kubectl apply -f -
          kubectl apply -f k8s/apis/secrets.yaml
          kubectl -n fraudstream-apis wait externalsecret --all --for=condition=Ready --timeout=2m
        '''
        withCredentials([usernamePassword(credentialsId: 'gateway',
                                          usernameVariable: 'GATEWAY_USER',
                                          passwordVariable: 'GATEWAY_PASSWORD')]) {
          sh '''
            helm upgrade --install inference-api k8s/charts/fraudstream-api \
              -n fraudstream-apis -f k8s/apis/inference-api.yaml \
              --set-string image.tag="$TAG" --rollback-on-failure --timeout 3m
            ./cicd/wait_for_version.sh inference.fraudstream.localhost "$TAG"
            ./cicd/check_gateway.sh inference.fraudstream.localhost \
              || { helm rollback inference-api -n fraudstream-apis --wait; exit 1; }
          '''
        }
      }
    }

    stage('Context API') {
      when { expression { deploying('api/src ml/src/fraudstream_ml rag/src/fraudstream_rag k8s/Dockerfile.context k8s/apis/context-api.yaml k8s/apis/secrets.yaml k8s/charts k8s/gateway cicd/gateway.sh cicd/check_gateway.sh') } }
      steps {
        sh '''
          docker build -f k8s/Dockerfile.context -t "fraudstream-context:$TAG" .
          kind load docker-image "fraudstream-context:$TAG" --name fraudstream
          kubectl create namespace fraudstream-apis --dry-run=client -o yaml | kubectl apply -f -
          kubectl apply -f k8s/apis/secrets.yaml
          kubectl -n fraudstream-apis wait externalsecret --all --for=condition=Ready --timeout=2m
        '''
        withCredentials([usernamePassword(credentialsId: 'gateway',
                                          usernameVariable: 'GATEWAY_USER',
                                          passwordVariable: 'GATEWAY_PASSWORD')]) {
          sh '''
            helm upgrade --install context-api k8s/charts/fraudstream-api \
              -n fraudstream-apis -f k8s/apis/context-api.yaml \
              --set-string image.tag="$TAG" --rollback-on-failure --timeout 4m
            ./cicd/wait_for_version.sh context.fraudstream.localhost "$TAG"
            ./cicd/check_gateway.sh context.fraudstream.localhost \
              || { helm rollback context-api -n fraudstream-apis --wait; exit 1; }
          '''
        }
      }
    }

    stage('Drift detection API') {
      when { expression { deploying('api/src api/reference k8s/Dockerfile.drift-detection k8s/apis/drift-detection.yaml k8s/charts k8s/gateway cicd/gateway.sh cicd/check_gateway.sh') } }
      steps {
        sh '''
          docker build -f k8s/Dockerfile.drift-detection -t "fraudstream-drift-detection:$TAG" .
          kind load docker-image "fraudstream-drift-detection:$TAG" --name fraudstream
        '''
        withCredentials([usernamePassword(credentialsId: 'gateway',
                                          usernameVariable: 'GATEWAY_USER',
                                          passwordVariable: 'GATEWAY_PASSWORD')]) {
          sh '''
            helm upgrade --install drift-detection k8s/charts/fraudstream-api \
              -n fraudstream-apis -f k8s/apis/drift-detection.yaml \
              --set-string image.tag="$TAG" --rollback-on-failure --timeout 3m
            ./cicd/wait_for_version.sh drift-detection.fraudstream.localhost "$TAG"
            ./cicd/check_gateway.sh drift-detection.fraudstream.localhost \
              || { helm rollback drift-detection -n fraudstream-apis --wait; exit 1; }
          '''
        }
      }
    }
    stage('Model server') {
      when { expression { deploying('serving/src k8s/Dockerfile.serving k8s/Dockerfile.model k8s/models cicd/apply_model.sh cicd/check_model_split.sh api/tools/model_split.py') } }
      steps {
        withCredentials([usernamePassword(credentialsId: 'gateway',
                                          usernameVariable: 'GATEWAY_USER',
                                          passwordVariable: 'GATEWAY_PASSWORD')]) {
          script {
            // The rollout job commits every step. A build of an older commit must not
            // apply its older split over a newer one; the newer commit's build applies it.
            // The lock is the one the rollout job holds while it changes the split, so the
            // check and the apply happen with nobody else moving the split in between.
            // shortcut: a file lock works because both jobs run on the one built-in node;
            // with build agents, use a Lockable Resources lock instead.
            def applied = sh(returnStatus: true, script: '''
              exec 9>"$JENKINS_HOME/model-split.lock"
              flock 9
              git fetch -q origin "$DEPLOY_BRANCH"
              git diff --quiet HEAD "origin/$DEPLOY_BRANCH" -- k8s/models/fraud-detection.yaml || exit 99
              ./cicd/apply_model.sh
            ''')
            if (applied == 99) {
              echo 'A newer commit changes the model manifest, so its build applies it.'
              return
            }
            if (applied != 0) {
              error("applying the model failed (exit ${applied})")
            }
            sh './cicd/check_model_split.sh'
            // Only a push that changed the model manifest starts a rollout, so an
            // unrelated commit never resumes a rollout a person has to decide on.
            def start = deploying('k8s/models/fraud-detection.yaml') && sh(returnStatus: true, script: '''
              cd monitoring && PYTHONPATH=src uv run python -m fraudstream_monitoring.rollout --should-start \
                --policy ../k8s/models/rollout.yaml --manifest ../k8s/models/fraud-detection.yaml \
                --subject "$(git log -1 --format=%s)"
            ''') == 0
            if (start) {
              build job: 'fraudstream-model-rollout', wait: false
            }
          }
        }
      }
    }

    stage('Training pipeline') {
      when { expression { deploying('ml/src pipelines/src feature_store/feature_repo k8s/Dockerfile.ml') } }
      steps {
        sh '''
          docker build -f k8s/Dockerfile.ml -t "fraudstream-ml:$TAG" .
          kind load docker-image "fraudstream-ml:$TAG" --name fraudstream
          kubectl -n kubeflow port-forward svc/ml-pipeline 8888:8888 &
          forward=$!
          trap "kill $forward" EXIT
          for i in $(seq 20); do curl -s -o /dev/null http://localhost:8888/apis/v2beta1/healthz && break; sleep 1; done
          cd pipelines
          PYTHONPATH=src TRAINING_IMAGE="fraudstream-ml:$TAG" uv run python -m fraudstream_pipelines.submit \
            --host http://localhost:8888 --upload-version "$TAG"
        '''
      }
    }

    stage('Airflow pipelines') {
      when { expression { deploying('airflow src configs feature_store/src feature_store/feature_repo monitoring api/src/fraudstream_api/drift_detection api/reference ml/src rag datahub/src datahub/contracts') } }
      steps {
        script {
          if (sh(script: 'test -d /deploy/.git', returnStatus: true) == 0) {
            unstable('AIRFLOW_PROJECT_DIR is not set, so the Airflow pipelines were not deployed')
            return
          }
          sh '''
            uv run python -m compileall -q airflow/dags
            ./cicd/sync_airflow.sh /deploy
          '''
          if (sh(script: 'docker ps --format "{{.Names}}" | grep -qx fraudstream-airflow-scheduler', returnStatus: true) != 0) {
            unstable('Airflow is not running: the pipelines were copied, but not checked')
            return
          }
          sh '''
            docker exec fraudstream-airflow-scheduler airflow dags reserialize
            errors=$(docker exec fraudstream-airflow-scheduler airflow dags list-import-errors)
            echo "$errors" | grep -q "No data found" || { echo "$errors"; exit 1; }
            dags=$(docker exec fraudstream-airflow-scheduler airflow dags list)
            for dag in fraudstream_raw_to_bronze fraudstream_bronze_to_silver_gold fraudstream_offline_features fraudstream_feature_store_materialize fraudstream_drift_monitoring fraudstream_rag_index; do
              echo "$dags" | grep -qw "$dag" || { echo "$dag is missing"; exit 1; }
            done
          '''
        }
      }
    }

    stage('RAG pipeline') {
      when { expression { deploying('rag airflow/dags/rag_index.py airflow/Dockerfile datahub/src datahub/contracts cicd/run_dag.sh') } }
      steps {
        script {
          // Building the image embeds one sentence with the pinned model, so a broken pin or wheel fails here.
          sh 'docker build -f airflow/Dockerfile -t "fraudstream-airflow:$TAG" -t fraudstream-airflow:local .'
          if (sh(script: 'test -d /deploy/.git', returnStatus: true) == 0) {
            unstable('AIRFLOW_PROJECT_DIR is not set, so the RAG pipeline was not run')
            return
          }
          withCredentials([usernamePassword(credentialsId: 'minio',
                                            usernameVariable: 'MINIO_ACCESS_KEY',
                                            passwordVariable: 'MINIO_SECRET_KEY')]) {
            sh '''
              cd rag
              MINIO_ENDPOINT=http://minio:9000 PYTHONPATH=src:../src uv run python -m fraudstream_rag.steps sync-handbook \
                --source handbook --target s3a://fraudstream/rag/handbook
            '''
          }
          if (sh(script: 'docker ps --format "{{.Names}}" | grep -qx fraudstream-airflow-scheduler', returnStatus: true) != 0) {
            unstable('Airflow is not running: the handbook was copied, but the pipeline was not run')
            return
          }
          // Recreating Airflow would kill running tasks, so a person does it.
          if (sh(script: 'test "$(docker inspect -f "{{.Image}}" fraudstream-airflow-scheduler)" = "$(docker image inspect -f "{{.Id}}" fraudstream-airflow:local)"', returnStatus: true) != 0) {
            unstable('Airflow runs an older image: recreate it with ./infra/vault/compose.sh --profile orchestration up -d')
          }
          sh '''
            docker exec fraudstream-airflow-scheduler airflow dags reserialize
            ./cicd/run_dag.sh fraudstream_rag_index 1800
          '''
        }
      }
    }

    stage('Stream push job') {
      when { expression { deploying('feature_store/src feature_store/feature_repo src') } }
      steps {
        script {
          if (sh(script: 'test -d /deploy/.git', returnStatus: true) == 0) {
            unstable('AIRFLOW_PROJECT_DIR is not set, so the stream push job was not deployed')
            return
          }
          sh './cicd/sync_airflow.sh /deploy'
          if (sh(script: 'docker ps --format "{{.Names}}" | grep -qx fraudstream-feast-stream-push', returnStatus: true) != 0) {
            unstable('The stream push job is not running: its code was copied, but not restarted')
            return
          }
          sh '''
            docker restart fraudstream-feast-stream-push
            sleep 30
            test "$(docker inspect -f '{{.State.Running}} {{.RestartCount}}' fraudstream-feast-stream-push)" = "true 0" \
              || { docker logs --tail 30 fraudstream-feast-stream-push; exit 1; }
          '''
        }
      }
    }

    stage('MCP servers') {
      when { expression { deploying(MCP_PATHS) } }
      steps {
        sh 'TAG="$TAG" ./k8s/mcp/install.sh && ./k8s/mcp/check.sh'
      }
    }

    stage('Context agent') {
      when { expression { deploying("${AGENT_PATHS} ${CONTEXT_AGENT_PATHS} ${MCP_PATHS}") } }
      environment {
        MLFLOW_TRACKING_URI = 'http://mlflow:5000'
        ROLLBACK_COMMIT = "${env.GIT_PREVIOUS_SUCCESSFUL_COMMIT ?: ''}"
      }
      steps {
        sh './cicd/deploy_agent.sh context-agent'
      }
    }

    stage('Risk agent') {
      when { expression { deploying("${AGENT_PATHS} ${RISK_AGENT_PATHS} ${MCP_PATHS}") } }
      environment {
        MLFLOW_TRACKING_URI = 'http://mlflow:5000'
        ROLLBACK_COMMIT = "${env.GIT_PREVIOUS_SUCCESSFUL_COMMIT ?: ''}"
      }
      steps {
        sh './cicd/deploy_agent.sh risk-agent'
      }
    }

    stage('Coordinator agent') {
      when { expression { deploying("${AGENT_PATHS} agents/prompts/coordinator-agent.md k8s/agents/coordinator-agent.yaml ${CONTEXT_AGENT_PATHS} ${RISK_AGENT_PATHS} ${MCP_PATHS}") } }
      environment {
        MLFLOW_TRACKING_URI = 'http://mlflow:5000'
        ROLLBACK_COMMIT = "${env.GIT_PREVIOUS_SUCCESSFUL_COMMIT ?: ''}"
      }
      steps {
        sh './cicd/deploy_agent.sh coordinator-agent'
        withCredentials([usernamePassword(credentialsId: 'gateway',
                                          usernameVariable: 'GATEWAY_USER',
                                          passwordVariable: 'GATEWAY_PASSWORD')]) {
          sh './cicd/check_gateway.sh kagent.fraudstream.localhost /health'
        }
      }
    }
  }

  post {
    always { cleanWs() }
  }
}

// Deploy only from the deploy branch, and only what this push touched. With no
// earlier successful build there is nothing to compare against, so deploy.
def deploying(String paths) {
  if (env.BRANCH_NAME != env.DEPLOY_BRANCH) { return false }
  if (params.DEPLOY_ALL || !env.GIT_PREVIOUS_SUCCESSFUL_COMMIT) { return true }
  return sh(
    script: "git diff --quiet ${env.GIT_PREVIOUS_SUCCESSFUL_COMMIT} ${env.GIT_COMMIT} -- ${paths}",
    returnStatus: true,
  ) != 0
}
