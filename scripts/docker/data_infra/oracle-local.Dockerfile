FROM gvenzl/oracle-free:latest
LABEL bighill="data"

USER root
# microdnf is a package manager with wget and unzip
RUN microdnf install -y wget unzip expect && \
    microdnf clean all

USER oracle
