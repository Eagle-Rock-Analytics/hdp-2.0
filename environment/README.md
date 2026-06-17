# Setting Up the Environment

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. Dependencies are declared in [`pyproject.toml`](../pyproject.toml).

## 1. Install uv

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 2. Install system dependencies for geospatial packages

`cartopy` requires GEOS and PROJ:

```bash
# Ubuntu/Debian
sudo apt install libgeos-dev libproj-dev

# macOS
brew install geos proj
```

## 3. Install project dependencies

From the repo root:

```bash
# Runtime deps only
uv sync

# With dev tools (black, ruff, pre-commit, pytest, etc.)
uv sync --extra dev
uv run pre-commit install
```

Or use the Makefile shortcut:

```bash
make install-dev
```

The virtual environment is created at `.venv/` and managed automatically by uv.


```bash
conda install mamba -c conda-forge -y
```

## 🛠️ 5. Install the required packages in the environment
Mamba will install all the packages in the `environment.yml` file (which includes python) into the `hist-obs` environment.

```bash
mamba env update --file environment.yml --prune -y
```

## 🧹 6. Clean up unnecessary packages and caches
```bash
conda clean --all -y
```
You're now ready to run code using the hist-obs environment!
