// Walks a model canary through k8s/models/rollout.yaml's steps. Separate from the
// deploy pipeline: a rollout takes hours, and the deploy allows 45 minutes.
pipeline {
  agent any

  options {
    timestamps()
    timeout(time: 12, unit: 'HOURS')
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  }

  environment {
    UV_CACHE_DIR = '/var/jenkins_home/uv-cache'
    DEPLOY_BRANCH = 'feature/e2e-fraudstream-platform'
  }

  stages {
    stage('Progressive rollout') {
      steps {
        withCredentials([
          usernamePassword(credentialsId: 'github', usernameVariable: 'GIT_USER', passwordVariable: 'GIT_TOKEN'),
          usernamePassword(credentialsId: 'gateway', usernameVariable: 'GATEWAY_USER', passwordVariable: 'GATEWAY_PASSWORD'),
        ]) {
          script {
            def status = sh(returnStatus: true, script: '''
              git checkout -q -B "$DEPLOY_BRANCH" "origin/$DEPLOY_BRANCH"
              git config user.name fraudstream-rollout
              git config user.email rollout@fraudstream.local
              # The token is read from the environment at push time and never written to disk.
              git config credential.helper '!f() { echo "username=$GIT_USER"; echo "password=$GIT_TOKEN"; }; f'
              kubectl -n observability port-forward svc/kps-prometheus 19090:9090 >/dev/null &
              forward=$!
              trap "kill $forward" EXIT
              for i in $(seq 20); do curl -s -o /dev/null http://localhost:19090/-/ready && break; sleep 1; done
              cd monitoring && PYTHONPATH=src uv run python -m fraudstream_monitoring.rollout \
                --policy ../k8s/models/rollout.yaml --manifest ../k8s/models/fraud-detection.yaml \
                --prometheus-url http://localhost:19090
            ''')
            if (status == 10) { error('the canary was unhealthy and was rolled back') }
            if (status == 11) { unstable('the rollout stopped without changing the split: no canary, or someone else changed it') }
            if (status == 12) { unstable('the canary behaves differently; a person decides') }
            // Anything else is a crash: the split stays wherever the last step left it.
            if (status != 0) { error("the rollout crashed (exit ${status}); check the split by hand") }
          }
        }
      }
    }
  }
}
