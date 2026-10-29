import runpy
from pathlib import Path
from unittest.mock import patch

import pytest
from packaging.specifiers import SpecifierSet


SETUP_PATH = Path(__file__).resolve().parents[2] / "setup.py"


@pytest.mark.parametrize("minor", range(10, 15))
def test_supported_python_versions(minor):
    with patch("sys.version_info", (3, minor, 0)), patch("setuptools.setup") as setup:
        runpy.run_path(str(SETUP_PATH), run_name="__main__")

    metadata = setup.call_args.kwargs
    assert f"3.{minor}" in SpecifierSet(metadata["python_requires"])
    assert f"Programming Language :: Python :: 3.{minor}" in metadata["classifiers"]
    assert "3.9" not in SpecifierSet(metadata["python_requires"])
    assert "3.15" not in SpecifierSet(metadata["python_requires"])


@pytest.mark.parametrize("version", [(3, 9, 0), (3, 15, 0), (4, 0, 0)])
def test_unsupported_python_versions(version, capsys):
    with patch("sys.version_info", version), patch("setuptools.setup") as setup:
        with pytest.raises(SystemExit) as exc:
            runpy.run_path(str(SETUP_PATH), run_name="__main__")

    assert exc.value.code == 1
    setup.assert_not_called()
    assert "Python 3.10 or higher but less than 3.15" in capsys.readouterr().out
