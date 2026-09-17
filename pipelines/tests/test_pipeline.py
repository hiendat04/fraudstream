"""Graph-shape tests for the training pipeline.

These compile the pipeline and inspect the result, so they need no cluster.
What they check is the wiring: which step feeds which, and whether the held-out
test data can reach anything it should not.
"""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml
from kfp import compiler

from fraudstream_pipelines.components import TRAINING_IMAGE
from fraudstream_pipelines.pipeline import fraud_training_pipeline


def _compile() -> dict:
    """Compile the pipeline once and hand back the parsed specification.

    Kubernetes-specific settings are written as a second document, so the two
    are merged here and the tests can treat it as one thing.
    """

    with TemporaryDirectory() as tmp:
        target = Path(tmp) / "pipeline.yaml"
        compiler.Compiler().compile(fraud_training_pipeline, str(target))
        merged: dict = {}
        for document in yaml.safe_load_all(target.read_text()):
            if document:
                merged.update(document)
        return merged


class CompiledPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spec = _compile()
        cls.tasks = cls.spec["root"]["dag"]["tasks"]

    def _producers_of(self, task_name: str) -> set[tuple[str, str]]:
        """Return the (step, output name) pairs a task takes as input."""

        artifacts = self.tasks[task_name].get("inputs", {}).get("artifacts", {})
        return {
            (spec["taskOutputArtifact"]["producerTask"], spec["taskOutputArtifact"]["outputArtifactKey"])
            for spec in artifacts.values()
            if "taskOutputArtifact" in spec
        }


class PipelineCompilesTest(CompiledPipeline):
    def test_it_compiles_to_a_specification(self):
        self.assertIn("root", self.spec)
        self.assertTrue(self.tasks, "the pipeline has no steps")

    def test_every_step_is_present(self):
        expected = {
            "retrieve",
            "prepare",
            "split",
            "train-baseline",
            "train-distributed",
            "evaluate",
            "save-bundle",
            "register-model",
        }
        self.assertEqual(expected, set(self.tasks))

    def test_every_step_runs_the_same_pinned_image(self):
        """One image for the whole pipeline, so no step can drift onto another build."""

        images = {
            executor["container"]["image"]
            for executor in self.spec["deploymentSpec"]["executors"].values()
            if "container" in executor
        }
        self.assertEqual({TRAINING_IMAGE}, images)


class LeakageTest(CompiledPipeline):
    def test_the_test_split_only_reaches_the_evaluate_step(self):
        """The held-out data must not be visible to anything that fits a model.

        Wiring it into a training step would inflate every score in the run
        while the pipeline still went green, so this is checked structurally
        rather than left to review.
        """

        consumers = {
            name
            for name in self.tasks
            if ("split", "test") in self._producers_of(name)
        }
        self.assertEqual({"evaluate"}, consumers)

    def test_neither_training_step_sees_the_test_split(self):
        for step in ("train-distributed", "train-baseline"):
            inputs = self._producers_of(step)
            self.assertNotIn(("split", "test"), inputs, f"{step} can see the test data")

    def test_the_threshold_is_chosen_on_validation_data(self):
        """Fitting the cut-off on test would be scoring and tuning on the same rows."""

        self.assertIn(("split", "validation"), self._producers_of("evaluate"))


class GraphShapeTest(CompiledPipeline):
    def test_the_two_training_steps_do_not_wait_for_each_other(self):
        """The baseline is a yardstick, not a stage -- it should run alongside."""

        distributed = self.tasks["train-distributed"].get("dependentTasks", [])
        baseline = self.tasks["train-baseline"].get("dependentTasks", [])
        self.assertNotIn("train-baseline", distributed)
        self.assertNotIn("train-distributed", baseline)

    def test_evaluate_waits_for_both_models(self):
        inputs = self._producers_of("evaluate")
        self.assertIn(("train-distributed", "model"), inputs)
        self.assertIn(("train-baseline", "model"), inputs)

    def test_the_bundle_saves_the_distributed_model(self):
        self.assertIn(("train-distributed", "model"), self._producers_of("save-bundle"))


class CredentialsTest(CompiledPipeline):
    def _steps_given_credentials(self) -> set[str]:
        executors = (
            self.spec.get("platforms", {})
            .get("kubernetes", {})
            .get("deploymentSpec", {})
            .get("executors", {})
        )
        return {
            name.removeprefix("exec-")
            for name, spec in executors.items()
            if spec.get("secretAsEnv")
        }

    def test_the_steps_that_read_storage_get_credentials(self):
        self.assertEqual({"retrieve", "train-distributed"}, self._steps_given_credentials())

    def test_no_other_step_is_handed_a_password(self):
        """Steps working purely on pipeline artifacts have no business holding secrets."""

        for step in ("prepare", "split", "train-baseline", "evaluate", "save-bundle"):
            self.assertNotIn(step, self._steps_given_credentials())


class VersioningTest(CompiledPipeline):
    def _parameter_sources(self, task_name: str) -> set[tuple[str, str]]:
        """Return the (step, output name) pairs a task takes as plain values."""

        parameters = self.tasks[task_name].get("inputs", {}).get("parameters", {})
        return {
            (spec["taskOutputParameter"]["producerTask"],
             spec["taskOutputParameter"]["outputParameterKey"])
            for spec in parameters.values()
            if "taskOutputParameter" in spec
        }

    def test_the_model_is_registered_against_the_data_it_used(self):
        """This is the link between the two halves of the phase.

        The registration step must take the snapshot id from the step that
        fetched the data. Without it a model version cannot be traced back to
        its rows.
        """

        sources = self._parameter_sources("register-model")
        producers = {step for step, _ in sources}
        self.assertIn("retrieve", producers)

    def test_registration_waits_for_the_scores(self):
        self.assertIn("evaluate", self.tasks["register-model"].get("dependentTasks", []))

    def test_registration_gets_the_trained_model(self):
        self.assertIn(("train-distributed", "model"), self._producers_of("register-model"))

    def test_registration_is_not_handed_any_passwords(self):
        """MLflow uploads the model itself, so this step needs no credentials."""

        executors = (
            self.spec.get("platforms", {})
            .get("kubernetes", {})
            .get("deploymentSpec", {})
            .get("executors", {})
        )
        self.assertNotIn("exec-register-model", {
            name for name, spec in executors.items() if spec.get("secretAsEnv")
        })


class ParametersTest(CompiledPipeline):
    def _defaults(self) -> dict:
        return self.spec["root"]["inputDefinitions"]["parameters"]

    def test_the_worker_count_is_a_parameter_not_a_constant(self):
        self.assertIn("num_nodes", self._defaults())

    def test_it_defaults_to_more_than_one_worker(self):
        """A default of one would make a distributed run look fine while not being one."""

        default = self._defaults()["num_nodes"].get("defaultValue")
        self.assertGreaterEqual(int(default), 2)

    def test_the_retrieval_window_is_a_parameter(self):
        for name in ("start_date", "end_date"):
            self.assertIn(name, self._defaults())

    def test_the_split_boundaries_match_the_notebook(self):
        defaults = self._defaults()
        self.assertEqual("2026-05-05", defaults["train_end"]["defaultValue"])
        self.assertEqual("2026-05-30", defaults["validation_end"]["defaultValue"])


if __name__ == "__main__":
    unittest.main()
