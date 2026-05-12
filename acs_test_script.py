import os, requests
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent

def _load_env_file(path: Path) -> None:
    """Load ``KEY=value`` lines into the environment if not already set (no extra deps)."""
    if not path.is_file():
        return
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return
    for raw in content.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        os.environ[key] = value


_load_env_file(_PROJECT_ROOT / ".env")

key = os.environ.get("CENSUS_API_KEY")
url = "https://api.census.gov/data/2023/acs/acs5"

c16002 = ["C16002_001E", "C16002_004E", "C16002_007E", "C16002_010E", "C16002_013E"]

for year in [2018, 2023]:
    url = f"https://api.census.gov/data/{year}/acs/acs5"
    params = {
        "get": "NAME," + ",".join(c16002),
        "for": "tract:*",
        "in": "state:36 county:005",
        "key": key,
    }
    r = requests.get(url, params=params, timeout=60)
    print(f"{year}: {r.status_code}")