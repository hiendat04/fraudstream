// Walks a model canary through k8s/models/rollout.yaml's steps. Separate from the
// deploy pipeline: a rollout takes hours, and the deploy allows 45 minutes.
// On a hold it waits for a person: continue, accepting the difference, or roll back.
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
          sh '''
            git checkout -q -B "$DEPLOY_BRANCH" "origin/$DEPLOY_BRANCH"
            git config user.name fraudstream-rollout
            git config user.email rollout@fraudstream.local
            # The token is read from the environment at push time and never written to disk.
            git config credential.helper '!f() { echo "username=$GIT_USER"; echo "password=$GIT_TOKEN"; }; f'
          '''
          script {
            def rollout = { String choice ->
              withEnv(["ROLLOUT_CHOICE=${choice}"]) {
                return sh(returnStatus: true, script: '''
                  kubectl -n observability port-forward svc/kps-prometheus 19090:9090 >/dev/null &
                  forward=$!
                  trap "kill $forward" EXIT
                  for i in $(seq 20); do curl -s -o /dev/null http://localhost:19090/-/ready && break; sleep 1; done
                  cd monitoring && PYTHONPATH=src uv run python -m fraudstream_monitoring.rollout \
                    --policy ../k8s/models/rollout.yaml --manifest ../k8s/models/fraud-detection.yaml \
                    --prometheus-url http://localhost:19090 --lock "$JENKINS_HOME/model-split.lock" $ROLLOUT_CHOICE
                ''')
              }
            }

            def status = rollout('')
            def rolledBackBy = ''
            while (status == 12) {
              def decision
              try {
                timeout(time: 4, unit: 'HOURS') {
                  decision = input(
                    message: 'The canary behaves differently (the numbers are in the log above). Decide:',
                    ok: 'Decide',
                    submitterParameter: 'DECIDED_BY',
                    parameters: [choice(name: 'DECISION', choices: ['Continue: accept the difference', 'Roll back'])],
                  )
                }
              } catch (err) {
                unstable('held, and nobody decided: the share stays where it is')
                return
              }
              echo "${decision.DECIDED_BY} chose: ${decision.DECISION}"
              rolledBackBy = decision.DECISION == 'Roll back' ? decision.DECIDED_BY : ''
              status = rollout(rolledBackBy ? '--roll-back' : '--accept-behaviour')
            }

            if (status == 10 && rolledBackBy) {
              unstable("rolled back to 0% by ${rolledBackBy}")
            } else if (status == 10) {
              error('the canary was unhealthy and was rolled back')
            } else if (status == 11) {
              unstable('the rollout stopped without changing the split: no canary, someone else changed it, or the deploy has not applied it yet')
            } else if (status != 0) {
              // A crash: the split stays wherever the last step left it.
              error("the rollout crashed (exit ${status}); check the split by hand")
            }
          }
        }
      }
    }
  }
}
