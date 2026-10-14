import pytest

from fraudstream_monitoring.rollout_manifest import (
    promoted,
    rolled_back,
    should_start,
    split,
    with_canary,
)

# A fixed copy of the manifest serving version 2 alone. The repo's own file changes with every rollout.
MANIFEST = """\
apiVersion: v1
kind: Namespace
metadata:
  name: kserve-models
---
apiVersion: serving.kserve.io/v1beta1
kind: InferenceService
metadata:
  name: fraud-detection
  namespace: kserve-models
spec:
  predictor:
    minReplicas: 1
    containers:
      - name: kserve-container
        image: dev.local/fraudstream-serving:model-2
        imagePullPolicy: IfNotPresent
        ports:
          - containerPort: 8080
            protocol: TCP
        resources:
          requests:
            cpu: 100m
            memory: 512Mi
          limits:
            memory: 2Gi
"""
PROGRESSIVE = {"mode": "progressive"}


def canary(percent, version="4"):
    return with_canary(MANIFEST.replace("serving:model-2", f"serving:model-{version}"), percent)


def test_a_round_trip_keeps_the_file_as_it_is():
    assert with_canary(canary(20), 20) == canary(20)
    assert promoted(promoted(MANIFEST)) == promoted(MANIFEST)


def test_the_split_is_read_from_the_manifest():
    assert split(MANIFEST) == ("2", None)
    assert split(canary(20)) == ("4", 20)


def test_a_step_changes_only_the_traffic_share():
    stepped = with_canary(canary(20), 40)
    assert split(stepped) == ("4", 40)
    assert stepped.replace("canaryTrafficPercent: 40", "canaryTrafficPercent: 20") == canary(20)


def test_promotion_removes_the_canary_and_keeps_a_warm_replica():
    """A served model that scales to zero makes its next payment wait for a cold start."""

    done = promoted(canary(60))
    assert split(done) == ("4", None)
    assert "minReplicas: 1" in done


def test_a_manifest_without_a_model_tag_is_refused():
    with pytest.raises(ValueError, match="model-<version>"):
        split(MANIFEST.replace("serving:model-2", "serving:dev"))


def test_a_rollback_sends_no_traffic_to_the_candidate():
    assert split(rolled_back(canary(60))) == ("4", 0)


def test_a_progressive_canary_starts_a_rollout():
    assert should_start(canary(20), PROGRESSIVE, "feat(models): start version 4 as a 20% canary")


def test_a_rollout_commit_never_starts_another_rollout():
    assert not should_start(canary(40), PROGRESSIVE, "rollout(models): version 4 at 40%")


def test_a_manual_policy_or_no_canary_starts_nothing():
    assert not should_start(canary(20), {"mode": "manual"}, "feat: x")
    assert not should_start(MANIFEST, PROGRESSIVE, "feat: x")
    assert not should_start(canary(0), PROGRESSIVE, "fix: roll back")
