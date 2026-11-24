# Airflow 3.3.0 + the pr-compliance package.
# Pinned base image; the package is installed into the image's own Python environment
# with uv, under Airflow's official constraints so shared libraries (pandas, pyarrow,
# requests, ...) stay at exactly the versions Airflow was tested with.
FROM apache/airflow:3.3.0

# Vendored copy of
# https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.13.txt
COPY --chown=airflow:root constraints-airflow-3.3.0-py3.13.txt /tmp/constraints.txt

# The image ships uv (/home/airflow/.local/bin/uv) and VIRTUAL_ENV=/home/airflow/.local,
# so `uv pip install` targets Airflow's environment. Runs as the non-root airflow user.
USER airflow
COPY --chown=airflow:root processing/pyproject.toml processing/README.md /opt/pr-compliance/
COPY --chown=airflow:root processing/src /opt/pr-compliance/src
RUN uv pip install --no-cache -c /tmp/constraints.txt /opt/pr-compliance \
    && python -c "import pr_compliance, pandera; print('pr_compliance', pr_compliance.__version__)"

COPY --chown=airflow:root processing/dags/ /opt/airflow/dags/
