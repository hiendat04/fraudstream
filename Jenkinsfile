pipeline {
  agent any

  options {
    timestamps()
    timeout(time: 45, unit: 'MINUTES')
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
        stage('root')          { steps { sh './ci/test.sh root' } }
        stage('ml')            { steps { sh './ci/test.sh ml' } }
        stage('pipelines')     { steps { sh './ci/test.sh pipelines' } }
        stage('feature_store') { steps { sh './ci/test.sh feature_store' } }
        stage('datahub')       { steps { sh './ci/test.sh datahub' } }
        stage('api')           { steps { sh './ci/test.sh api' } }
        stage('serving')       { steps { sh './ci/test.sh serving' } }
      }
    }

    stage('Inference API') {
      when { expression { deploying('api/src ml/src/fraudstream_ml k8s/Dockerfile.inference k8s/apis/inference-api.yaml k8s/charts') } }
      steps {
        sh '''
          docker build -f k8s/Dockerfile.inference -t "fraudstream-inference:$TAG" .
          kind load docker-image "fraudstream-inference:$TAG" --name fraudstream
          kubectl create namespace fraudstream-apis --dry-run=client -o yaml | kubectl apply -f -
        '''
        withCredentials([usernamePassword(credentialsId: 'postgres',
                                          usernameVariable: 'PGUSER',
                                          passwordVariable: 'PGPASSWORD')]) {
          sh '''
            kubectl -n fraudstream-apis create secret generic feature-store-registry \
              --from-literal=POSTGRES_USER="$PGUSER" \
              --from-literal=POSTGRES_PASSWORD="$PGPASSWORD" \
              --dry-run=client -o yaml | kubectl apply -f -
          '''
        }
        sh '''
          helm upgrade --install inference-api k8s/charts/fraudstream-api \
            -n fraudstream-apis -f k8s/apis/inference-api.yaml \
            --set-string image.tag="$TAG" --rollback-on-failure --timeout 3m
          ./ci/wait_for_version.sh inference.localhost "$TAG"
        '''
      }
    }

    stage('Drift detection API') {
      when { expression { deploying('api/src api/reference k8s/Dockerfile.drift-detection k8s/apis/drift-detection.yaml k8s/charts') } }
      steps {
        sh '''
          docker build -f k8s/Dockerfile.drift-detection -t "fraudstream-drift-detection:$TAG" .
          kind load docker-image "fraudstream-drift-detection:$TAG" --name fraudstream
          kubectl create namespace fraudstream-apis --dry-run=client -o yaml | kubectl apply -f -
          helm upgrade --install drift-detection k8s/charts/fraudstream-api \
            -n fraudstream-apis -f k8s/apis/drift-detection.yaml \
            --set-string image.tag="$TAG" --rollback-on-failure --timeout 3m
          ./ci/wait_for_version.sh drift-detection.localhost "$TAG"
        '''
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
