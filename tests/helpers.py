import importlib.util
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
# Example metric IDs used by the tests. They are placeholders, not real IDs.
METRICS = ["MetricA1", "MetricB2", "MetricC3"]
METRICS_CSV = ",".join(METRICS)


def load_script(name, filename):
    path = SCRIPTS / filename
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
