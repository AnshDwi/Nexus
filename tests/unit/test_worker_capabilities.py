import pytest

from nexus.domain.worker_capabilities import GpuCapability, WorkerCapabilities


def _make_capabilities(**overrides):
    defaults = {
        "cpu_cores": 4.0,
        "memory_mb": 8192,
        "architecture": "x86_64",
        "region": "us-east",
        "runtime": "python3.12",
    }
    defaults.update(overrides)
    return WorkerCapabilities(**defaults)


def test_worker_capabilities_accepts_valid_values():
    capabilities = _make_capabilities()
    assert capabilities.gpu is None
    assert capabilities.custom == {}


def test_worker_capabilities_accepts_gpu_and_custom():
    capabilities = _make_capabilities(
        gpu=GpuCapability(model="A100", count=2, memory_mb=40960),
        custom={"warm_cache_for_tenant": "acme"},
    )
    assert capabilities.gpu is not None
    assert capabilities.gpu.count == 2
    assert capabilities.custom["warm_cache_for_tenant"] == "acme"


@pytest.mark.parametrize(
    "overrides",
    [
        {"cpu_cores": 0},
        {"cpu_cores": -1},
        {"memory_mb": 0},
        {"memory_mb": -1},
        {"architecture": ""},
        {"region": ""},
        {"runtime": ""},
    ],
)
def test_worker_capabilities_rejects_invalid_values(overrides):
    with pytest.raises(ValueError):
        _make_capabilities(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"count": 0},
        {"count": -1},
        {"memory_mb": 0},
        {"memory_mb": -1},
    ],
)
def test_gpu_capability_rejects_invalid_values(overrides):
    defaults = {"model": "A100", "count": 1, "memory_mb": 40960}
    defaults.update(overrides)
    with pytest.raises(ValueError):
        GpuCapability(**defaults)
