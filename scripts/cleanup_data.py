#!/usr/bin/env python3
"""
Utility script to clean up Streamflow pipeline data directories.
"""

import argparse
import os
import shutil
from pathlib import Path

# Target folders relative to project root
TARGET_SUBDIRS = ["raw", "curated", "rejects", "checkpoints"]


def get_project_root() -> Path:
    # This script is located in <project_root>/scripts/cleanup_data.py
    return Path(__file__).resolve().parent.parent


def clean_directory(dir_path: Path, dry_run: bool = False) -> int:
    """
    Cleans all files and subdirectories within the target directory,
    except for the .gitkeep file. Returns the count of deleted items.
    """
    deleted_count = 0
    if not dir_path.exists():
        if not dry_run:
            print(f"Directory {dir_path} does not exist. Creating it.")
            dir_path.mkdir(parents=True, exist_ok=True)
            gitkeep = dir_path / ".gitkeep"
            gitkeep.touch()
        else:
            print(f"Directory {dir_path} does not exist. Would create it.")
        return 0

    print(f"Processing directory: {dir_path.relative_to(get_project_root())}")
    
    # Iterate through directory items
    for item in dir_path.iterdir():
        if item.name == ".gitkeep":
            continue
        
        if dry_run:
            print(f"  [DRY RUN] Would delete: {item.relative_to(get_project_root())}")
            deleted_count += 1
        else:
            try:
                if item.is_dir() and not item.is_symlink():
                    shutil.rmtree(item)
                else:
                    item.unlink()
                print(f"  Deleted: {item.relative_to(get_project_root())}")
                deleted_count += 1
            except Exception as e:
                print(f"  Error deleting {item}: {e}")

    # Ensure a .gitkeep file exists
    gitkeep = dir_path / ".gitkeep"
    if not gitkeep.exists() and not dry_run:
        gitkeep.touch()
        print("  Recreated missing .gitkeep")

    return deleted_count


def main():
    parser = argparse.ArgumentParser(
        description="Clean up pipeline data directories (raw, curated, rejects, checkpoints)."
    )
    parser.add_argument(
        "--only",
        choices=TARGET_SUBDIRS,
        help="Clean only a specific directory (e.g., 'raw' or 'checkpoints')."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what files/folders would be deleted without actually deleting them."
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip interactive confirmation prompt."
    )
    
    args = parser.parse_args()
    project_root = get_project_root()
    data_dir = project_root / "data"

    # Select target folders
    selected_targets = [args.only] if args.only else TARGET_SUBDIRS
    target_paths = {t: data_dir / t for t in selected_targets}

    print("=== Streamflow Data Directory Cleanup ===")
    print(f"Project root: {project_root}")
    print(f"Selected targets: {', '.join(selected_targets)}")
    if args.dry_run:
        print("[DRY RUN] No files will be deleted.")
    print("=========================================")

    # Interactive confirmation if not bypassed
    if not args.yes and not args.dry_run:
        confirm = input("Are you sure you want to proceed with deletion? (y/N): ").strip().lower()
        if confirm not in ("y", "yes"):
            print("Cleanup cancelled.")
            return

    # Execute cleanup
    total_deleted = 0
    for name, path in target_paths.items():
        total_deleted += clean_directory(path, dry_run=args.dry_run)

    print("=========================================")
    if args.dry_run:
        print(f"Dry run complete. Would delete {total_deleted} items.")
    else:
        print(f"Cleanup complete. Deleted {total_deleted} items.")


if __name__ == "__main__":
    main()
