import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[4]
CHART = ROOT / "deploy/helm/rr"
JOB_TEMPLATE = CHART / "templates/job-cluster-form.yaml"
STATEFULSET_TEMPLATE = CHART / "templates/statefulset.yaml"
VALUES = CHART / "values.yaml"
RAW_STATEFULSET = ROOT / "deploy/k8s/statefulset.yaml"
RAW_ADOPTION_STATEFULSET = ROOT / "deploy/k8s/statefulset-adopt.yaml"
RAW_JOB = ROOT / "deploy/k8s/cluster-form-job.yaml"
PLACEHOLDER = "REPLACE_WITH_FORMATION_CAPABLE_TAG"
HELM = shutil.which("helm")


class StaticDeploymentContractTests(unittest.TestCase):
    def test_adopt_job_is_an_ordinary_conditional_resource(self):
        source = JOB_TEMPLATE.read_text()
        self.assertIn('eq .Values.cluster.formation.mode "adopt"', source)
        self.assertIn("eq (int $rolloutReplicas) (int .Values.replicaCount)", source)
        self.assertNotIn('"helm.sh/hook"', source)
        self.assertNotIn('"helm.sh/hook-weight"', source)
        self.assertNotIn('"helm.sh/hook-delete-policy"', source)

    def test_adopt_job_name_is_revision_specific(self):
        source = JOB_TEMPLATE.read_text()
        self.assertIn(
            "name: {{ include \"rr.fullname\" . }}-cluster-form-{{ .Release.Revision }}",
            source,
        )

    def test_helm_default_image_tag_is_empty(self):
        values = yaml.safe_load(VALUES.read_text())
        self.assertEqual("", values["image"]["tag"])

    def test_helm_rejects_missing_and_0_2_0_image_tags(self):
        source = STATEFULSET_TEMPLATE.read_text()
        self.assertIn("not .Values.image.tag", source)
        self.assertIn('eq .Values.image.tag "0.2.0"', source)
        self.assertIn("explicitly set image.tag to a formation-capable image", source)

    def test_raw_manifests_require_formation_capable_image_override(self):
        for path in (RAW_STATEFULSET, RAW_ADOPTION_STATEFULSET, RAW_JOB):
            with self.subTest(path=path):
                source = path.read_text()
                self.assertIn(PLACEHOLDER, source)
                self.assertNotIn("ghcr.io/gardenias/rr:0.2.0", source)

    def test_helm_adoption_has_an_explicit_initialized_only_phase(self):
        values = yaml.safe_load(VALUES.read_text())
        self.assertIn("adoptionInitializedReplicas", values["cluster"]["formation"])
        self.assertIsNone(values["cluster"]["formation"]["adoptionInitializedReplicas"])
        statefulset = STATEFULSET_TEMPLATE.read_text()
        job = JOB_TEMPLATE.read_text()
        self.assertIn("adoptionInitializedReplicas", statefulset)
        self.assertIn("adoptionInitializedReplicas", job)

    def test_raw_adoption_starts_initialized_nodes_without_changing_the_seed(self):
        self.assertTrue(RAW_ADOPTION_STATEFULSET.exists(), "raw adoption phase-one manifest is missing")
        docs = [doc for doc in yaml.safe_load_all(RAW_ADOPTION_STATEFULSET.read_text()) if doc]
        statefulset = next(doc for doc in docs if doc.get("kind") == "StatefulSet")
        configmap = next(doc for doc in docs if doc.get("kind") == "ConfigMap")
        config = configmap["data"]["config.toml.tmpl"]
        self.assertEqual(1, statefulset["spec"]["replicas"])
        self.assertIn('mode = "adopt"', config)
        self.assertIn(
            'nodes = ["1@rr-0.rr:51001", "2@rr-1.rr:51001", "3@rr-2.rr:51001"]',
            config,
        )


@unittest.skipUnless(HELM, "helm binary is required for render assertions")
class HelmRenderTests(unittest.TestCase):
    def render(self, *assignments, revision=None):
        with tempfile.TemporaryDirectory(prefix="rr-helm-test-") as temp_dir:
            chart = CHART
            if revision is not None:
                chart = Path(temp_dir) / "rr"
                shutil.copytree(CHART, chart)
                job_template = chart / "templates/job-cluster-form.yaml"
                source = job_template.read_text()
                self.assertEqual(1, source.count(".Release.Revision"))
                job_template.write_text(source.replace(".Release.Revision", str(revision)))
            command = [
                HELM,
                "template",
                "rr",
                str(chart),
                "--set",
                "security.allowInsecure=true",
            ]
            for assignment in assignments:
                command.extend(["--set", assignment])
            return subprocess.run(command, text=True, capture_output=True)

    def render_ok(self, *assignments, revision=None):
        result = self.render(*assignments, revision=revision)
        self.assertEqual(0, result.returncode, result.stderr)
        return [doc for doc in yaml.safe_load_all(result.stdout) if doc]

    def test_genesis_renders_no_job(self):
        docs = self.render_ok("image.tag=formation-test")
        self.assertFalse(any(doc.get("kind") == "Job" for doc in docs))

    def test_adoption_phase_one_omits_pristine_replicas_and_job(self):
        docs = self.render_ok(
            "image.tag=formation-test",
            "cluster.formation.mode=adopt",
            "cluster.formation.adoptionInitializedReplicas=1",
        )
        statefulset = next(doc for doc in docs if doc.get("kind") == "StatefulSet")
        configmap = next(doc for doc in docs if doc.get("kind") == "ConfigMap")
        self.assertEqual(1, statefulset["spec"]["replicas"])
        self.assertFalse(any(doc.get("kind") == "Job" for doc in docs))
        config = configmap["data"]["config.toml.tmpl"]
        self.assertIn("1@rr-0.rr:51001", config)
        self.assertIn("2@rr-1.rr:51001", config)
        self.assertIn("3@rr-2.rr:51001", config)

    def test_adoption_phase_two_starts_full_roster_and_job(self):
        docs = self.render_ok(
            "image.tag=formation-test",
            "cluster.formation.mode=adopt",
            "cluster.formation.adoptionInitializedReplicas=3",
        )
        statefulset = next(doc for doc in docs if doc.get("kind") == "StatefulSet")
        self.assertEqual(3, statefulset["spec"]["replicas"])
        self.assertEqual(1, sum(doc.get("kind") == "Job" for doc in docs))

    def test_adopt_job_has_no_hook_annotations(self):
        docs = self.render_ok("image.tag=formation-test", "cluster.formation.mode=adopt")
        jobs = [doc for doc in docs if doc.get("kind") == "Job"]
        self.assertEqual(1, len(jobs))
        annotations = jobs[0]["metadata"].get("annotations", {})
        self.assertFalse(any(key.startswith("helm.sh/hook") for key in annotations))

    def test_adopt_job_names_differ_across_revisions(self):
        assignments = ("image.tag=formation-test", "cluster.formation.mode=adopt")
        first = self.render_ok(*assignments, revision=1)
        second = self.render_ok(*assignments, revision=2)
        first_name = next(doc for doc in first if doc.get("kind") == "Job")["metadata"]["name"]
        second_name = next(doc for doc in second if doc.get("kind") == "Job")["metadata"]["name"]
        self.assertNotEqual(first_name, second_name)
        self.assertTrue(first_name.endswith("-cluster-form-1"), first_name)
        self.assertTrue(second_name.endswith("-cluster-form-2"), second_name)

    def test_missing_image_tag_fails_rendering(self):
        result = self.render()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("explicitly set image.tag to a formation-capable image", result.stderr)

    def test_0_2_0_image_tag_fails_rendering(self):
        result = self.render("image.tag=0.2.0")
        self.assertNotEqual(0, result.returncode)
        self.assertIn("0.2.0", result.stderr)

    def test_explicit_formation_tag_renders(self):
        docs = self.render_ok("image.tag=formation-test")
        statefulsets = [doc for doc in docs if doc.get("kind") == "StatefulSet"]
        self.assertEqual(1, len(statefulsets))
        image = statefulsets[0]["spec"]["template"]["spec"]["containers"][0]["image"]
        self.assertEqual("ghcr.io/gardenias/rr:formation-test", image)


if __name__ == "__main__":
    unittest.main()
