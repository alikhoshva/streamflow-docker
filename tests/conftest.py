import sys
from pathlib import Path
import pytest

# Add project root to sys.path to allow importing from scripts
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.cleanup_data import clean_directory, TARGET_SUBDIRS


@pytest.fixture(scope="session", autouse=True)
def clean_data_directories():
    """
    Autouse session fixture that runs before any tests start.
    Cleans up raw, curated, rejects, and checkpoints to ensure a clean state.
    """
    print("\n--- [Pytest Session Start] Cleaning Streamflow Data Directories ---")
    data_dir = PROJECT_ROOT / "data"
    
    total_deleted = 0
    for name in TARGET_SUBDIRS:
        path = data_dir / name
        total_deleted += clean_directory(path, dry_run=False)
        
    print(f"--- [Pytest Session Start] Cleanup Complete. Deleted {total_deleted} items. ---\n")
