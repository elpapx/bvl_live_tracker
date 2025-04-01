# scraper/config.py
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "scraper" / "data"
LOGS_DIR = PROJECT_ROOT / "scraper" / "logs"