from pathlib import Path

import yaml

K8S_DIR = Path(__file__).resolve().parents[2] / "k8s"
WORKLOADS = {"Deployment", "Job", "CronJob"}


def load_manifests():
    docs = []
    for path in sorted(K8S_DIR.rglob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if doc:
                docs.append((path.name, doc))
    return docs


def pod_spec(doc):
    if doc["kind"] == "CronJob":
        return doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    return doc["spec"]["template"]["spec"]


def containers(doc):
    spec = pod_spec(doc)
    return spec["containers"] + spec.get("initContainers", [])


def workloads():
    return [(n, d) for n, d in load_manifests() if d["kind"] in WORKLOADS]


def test_there_are_manifests_and_each_has_kind_and_name():
    docs = load_manifests()
    assert len(docs) >= 10
    for name, doc in docs:
        assert doc.get("apiVersion"), name
        assert doc.get("kind"), name
        meta = doc["metadata"]
        assert meta.get("name") or meta.get("generateName"), name


def test_everything_lives_in_the_txn_namespace():
    for name, doc in load_manifests():
        if doc["kind"] != "Namespace":
            assert doc["metadata"].get("namespace") == "txn", name


def test_every_container_sets_resource_requests_and_a_memory_limit():
    for name, doc in workloads():
        for c in containers(doc):
            resources = c.get("resources", {})
            assert resources.get("requests"), f"{name}/{c['name']} has no requests"
            assert "memory" in resources.get("limits", {}), f"{name}/{c['name']} has no limit"


def test_images_are_pinned_and_never_latest():
    for name, doc in workloads():
        for c in containers(doc):
            image = c["image"]
            assert ":" in image, f"{name}: {image} has no tag"
            assert not image.endswith(":latest"), f"{name}: {image} uses latest"


def test_the_api_has_readiness_and_liveness_probes():
    api = next(d for _, d in workloads() if d["metadata"]["name"] == "api")
    main = pod_spec(api)["containers"][0]
    assert "readinessProbe" in main
    assert "livenessProbe" in main


def test_pods_do_not_inject_service_link_environment_variables():
    for name, doc in workloads():
        assert pod_spec(doc).get("enableServiceLinks") is False, name


def test_the_database_url_comes_from_a_secret_not_a_literal():
    for name, doc in workloads():
        for c in containers(doc):
            for env in c.get("env", []):
                if env["name"] == "DATABASE_URL":
                    assert "secretKeyRef" in env.get("valueFrom", {}), name
