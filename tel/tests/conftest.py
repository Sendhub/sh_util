import importlib
import os
import sys
import sysconfig
from pathlib import Path


def _import_stdlib_json_safely():
    """
    Deterministically ensure the stdlib `json` package is imported for tests,
    by temporarily removing the repository root and tests dir (the places we
    know local json.py may live) from sys.path while importing, then restoring.
    """
    stdlib_dir = sysconfig.get_paths().get("stdlib")
    if not stdlib_dir:
        raise RuntimeError("Cannot locate stdlib directory via sysconfig.")

    orig_sys_path = list(sys.path)

    # make stdlib high priority
    try:
        if stdlib_dir in sys.path:
            sys.path.remove(stdlib_dir)
    except Exception:
        pass
    sys.path.insert(0, stdlib_dir)

    # Determine repo/project root safely based on this file's location:
    # conftest is at <repo>/tel/tests/conftest.py
    # parents: 0=conftest.py, 1=tests, 2=tel, 3=<repo_root> (sh_util)
    this_file = Path(__file__).resolve()
    try:
        repo_root = this_file.parents[3]
    except IndexError:
        # fallback: two levels up (best-effort)
        repo_root = this_file.parents[2]

    repo_root_str = str(repo_root)
    tests_dir_str = str(this_file.parents[1])  # .../tel/tests

    to_remove = []
    # Remove repo root and tests dir if present (these are common shadow sources)
    for p in list(sys.path):
        if not p:
            # blank entry refers to current working dir; skip here
            continue
        try:
            # normalize actual path (resolve symlinks)
            if os.path.samefile(os.path.abspath(p), os.path.abspath(repo_root_str)):
                to_remove.append(p)
                continue
            if os.path.samefile(os.path.abspath(p), os.path.abspath(tests_dir_str)):
                to_remove.append(p)
                continue
        except Exception:
            # If samefile fails (e.g. non-existent), fall back to string compare
            if os.path.abspath(p) == os.path.abspath(repo_root_str) or os.path.abspath(p) == os.path.abspath(tests_dir_str):
                to_remove.append(p)

    # Remove all found entries (temporarily)
    removed_entries = []
    for p in to_remove:
        if p in sys.path:
            try:
                idx = sys.path.index(p)
            except ValueError:
                idx = None
            removed_entries.append((p, idx))
            sys.path.remove(p)

    # Try importing stdlib json now
    try:
        json_mod = importlib.import_module("json")
    except Exception as e:
        # restore and raise with context
        sys.path[:] = orig_sys_path
        raise RuntimeError(f"Failed to import json after removing {removed_entries}: {e}\n"
                           f"sys.path head: {orig_sys_path[:10]}") from e

    # If we still didn't get stdlib json, restore and explain
    if not hasattr(json_mod, "load"):
        sys.path[:] = orig_sys_path
        raise RuntimeError(
            "Imported module 'json' does not have 'load' even after removing project paths. "
            f"json.__file__={getattr(json_mod, '__file__', None)} removed_entries={removed_entries}"
        )

    # Restore original sys.path
    sys.path[:] = orig_sys_path

    # Keep the correct stdlib json loaded in sys.modules so later imports return it.
    print("stdlib json successfully imported from:", getattr(json_mod, "__file__", "<builtin>"))
    # For convenience, also print what we removed (only prints in verbose runs)
    if removed_entries:
        print("Temporarily removed sys.path entries while importing stdlib json:", removed_entries)

# Execute early
_import_stdlib_json_safely()
