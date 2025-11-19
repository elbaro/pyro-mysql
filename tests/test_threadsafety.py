"""Static code analysis tests to ensure thread safety guarantees."""

import pathlib

import pytest

# List of files that must not contain '&mut self' to guarantee thread safety
# These classes are exposed to Python and should be thread-safe
THREADSAFE_FILES = [
    # Async API - uses Arc<RwLock<>> for internal synchronization
    "src/async/conn.rs",
    "src/async/transaction.rs",
    # Sync API - uses parking_lot::RwLock for internal synchronization
    "src/sync/conn.rs",
    "src/sync/transaction.rs",
]


@pytest.mark.parametrize("file_path", THREADSAFE_FILES)
def test_no_mut_self(file_path):
    """
    Ensure file has no '&mut self' references to guarantee thread safety.

    All methods must use '&self' to maintain thread safety.
    Internal mutability should be handled through Arc<RwLock<>> or parking_lot::RwLock.
    """
    repo_root = pathlib.Path(__file__).parent.parent
    target_file = repo_root / file_path

    assert target_file.exists(), f"File not found: {target_file}"

    content = target_file.read_text()

    assert "&mut self" not in content, (
        f"Found '&mut self' in {file_path} which violates thread safety. "
        "All methods must use '&self' to maintain thread safety. "
        "Use internal synchronization primitives (Arc<RwLock<>> or parking_lot::RwLock) instead."
    )
