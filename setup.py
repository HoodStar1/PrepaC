from pathlib import Path
from setuptools import Extension, setup
from Cython.Build import cythonize

APP_DIR = Path("app")
EXCLUDE = {"__init__.py", "name_randomizer_data.py", "version.py"}
module_files = sorted(
    p for p in APP_DIR.glob("*.py")
    if p.name not in EXCLUDE
)

extensions = [
    Extension(
        name=f"app.{p.stem}",
        sources=[str(p)],
    )
    for p in module_files
]

setup(
    name="PrepaC",
    ext_modules=cythonize(
        extensions,
        compiler_directives={"language_level": "3", "embedsignature": False},
        annotate=False,
    ),
    zip_safe=False,
)
