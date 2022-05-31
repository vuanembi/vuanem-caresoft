FROM gcr.io/dataflow-templates-base/python39-template-launcher-base

ENV DIR="/template"

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${DIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${DIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${DIR}/main.py"

COPY . ${DIR}

RUN python $FLEX_TEMPLATE_PYTHON_SETUP_FILE install
    
ENV PIP_NO_DEPS=True
