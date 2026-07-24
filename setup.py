import os
from pathlib import Path

from setuptools import Extension, setup

APP_DIR = Path("app")
EXCLUDE = {"__init__.py", "name_randomizer_data.py", "version.py"}


def _build_extensions():
    """Build optional Cython extensions only for the hardened Docker image."""
    if os.environ.get("PREPAC_BUILD_CYTHON") != "1":
        return []

    try:
        from Cython.Build import cythonize
    except ImportError as exc:
        raise RuntimeError(
            "PREPAC_BUILD_CYTHON=1 requires Cython==3.2.8 to be installed"
        ) from exc

    module_files = sorted(
        path for path in APP_DIR.glob("*.py") if path.name not in EXCLUDE
    )
    extensions = [
        Extension(name=f"app.{path.stem}", sources=[str(path)])
        for path in module_files
    ]
    return cythonize(
        extensions,
        compiler_directives={"language_level": "3", "embedsignature": False},
        annotate=False,
    )

setup(
    ext_modules=_build_extensions(),
    zip_safe=False,
)
