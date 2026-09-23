pipeline {
  agent any

  options {
    timestamps()
    timeout(time: 45, unit: 'MINUTES')
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '20'))
  }

  environment {
    TAG = "${env.GIT_COMMIT.take(7)}"
    UV_CACHE_DIR = '/var/jenkins_home/uv-cache'
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
  }

  post {
    always { cleanWs() }
  }
}
