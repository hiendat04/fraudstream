FROM jenkins/jenkins:lts-jdk17

USER root

ARG KUBECTL_VERSION=v1.34.0
ARG KIND_VERSION=v0.30.0
ARG HELM_VERSION=v4.2.4
ARG COMPOSE_VERSION=v5.5.1
ARG BUILDX_VERSION=v0.37.1
ARG DOCKER_VERSION=29.8.1

# build-essential: psutil (serving) and crosshair-tool (api) publish no Linux
# arm64 wheels, so uv compiles them from source.
RUN apt-get update \
 && apt-get install --yes --no-install-recommends rsync build-essential \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# The Debian docker.io package ships only dockerd, not the docker CLI, so the
# client, compose and buildx (Docker's own apt repo, not Debian's) are fetched as
# static binaries the same way kubectl, kind and helm are. Without buildx the
# legacy builder runs and warns that it is deprecated.
RUN ARCH=$(dpkg --print-architecture) \
 && case "$ARCH" in arm64) DOCKER_ARCH=aarch64 ;; amd64) DOCKER_ARCH=x86_64 ;; *) echo "unsupported arch: $ARCH" >&2; exit 1 ;; esac \
 && curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl" \
 && curl -fsSLo /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-${ARCH}" \
 && curl -fsSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-${ARCH}.tar.gz" | tar xz -C /tmp \
 && mv "/tmp/linux-${ARCH}/helm" /usr/local/bin/helm \
 && curl -fsSL "https://download.docker.com/linux/static/stable/${DOCKER_ARCH}/docker-${DOCKER_VERSION}.tgz" | tar xz -C /tmp \
 && mv /tmp/docker/docker /usr/local/bin/docker \
 && mkdir -p /usr/local/lib/docker/cli-plugins \
 && curl -fsSLo /usr/local/lib/docker/cli-plugins/docker-compose \
      "https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-${DOCKER_ARCH}" \
 && curl -fsSLo /usr/local/lib/docker/cli-plugins/docker-buildx \
      "https://github.com/docker/buildx/releases/download/${BUILDX_VERSION}/buildx-${BUILDX_VERSION}.linux-${ARCH}" \
 && chmod +x /usr/local/bin/kubectl /usr/local/bin/kind /usr/local/bin/helm /usr/local/bin/docker \
      /usr/local/lib/docker/cli-plugins/docker-compose /usr/local/lib/docker/cli-plugins/docker-buildx \
 && rm -rf /tmp/docker /tmp/linux-${ARCH}

COPY --from=ghcr.io/astral-sh/uv:0.9.10 /uv /usr/local/bin/uv

COPY plugins.txt /usr/share/jenkins/ref/plugins.txt
RUN jenkins-plugin-cli --plugin-file /usr/share/jenkins/ref/plugins.txt

ENV JAVA_OPTS="-Djenkins.install.runSetupWizard=false"
ENV CASC_JENKINS_CONFIG=/var/jenkins_home/casc/jenkins.yaml
