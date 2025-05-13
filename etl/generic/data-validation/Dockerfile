# Use OpenJDK 11 base image
FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:latest as template_launcher
FROM apache/beam_python3.9_sdk:2.63.0 as beam_python
FROM openjdk:11-jre-slim

# Copy files from official SDK image, including script/dependencies.
COPY --from=beam_python /opt/apache/beam /opt/apache/beam
COPY --from=template_launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Install Python 3 and necessary dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip unzip libaio1 && \
    apt-get clean

# Create symlink to ensure 'python' points to 'python3'
RUN ln -s /usr/bin/python3 /usr/bin/python

COPY . /template

RUN unzip /template/instantclient.zip -d /template/ && \
    rm -f /template/*.zip

# Install Apache Beam for Python
RUN pip --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org install apache-beam[gcp]==2.63.0
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir -r "/template/requirements.txt" \
    && pip download --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --no-cache-dir --dest /tmp/dataflow-requirements-cache -r "/template/requirements.txt" \
    && pip freeze

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV ORACLE_PATH="/template/instantclient_21_17"
ENV LD_LIBRARY_PATH="/template/instantclient_21_17:$LD_LIBRARY_PATH"

RUN java -version && python --version

# Set the default command to run the Python pipeline
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]