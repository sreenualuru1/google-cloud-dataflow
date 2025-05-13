FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest as template_launcher
FROM apache/beam_python3.11_sdk:2.60.0

# Copy files from official SDK image, including script or dependencies.
COPY --from=apache/beam_python3.11_sdk /opt/apache/beam /opt/apache/beam

COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

RUN echo "testing pipeline trigger"
# Set main python file location and requirements file location
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/bicc_bq_merge_inc.py"

COPY . /template

RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir --upgrade pip \
    && pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir -r "/template/requirements.txt" \
    && pip download --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir --dest /tmp/dataflow-requirements-cache -r "/template/requirements.txt" \
    && pip freeze

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
