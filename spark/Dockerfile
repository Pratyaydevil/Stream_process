FROM apache/spark:3.5.3-python3
USER root
RUN pip install --no-cache-dir requests==2.32.3 scikit-learn==1.3.2 numpy==1.24.4
COPY jobs /opt/spark/jobs
USER 185
ENTRYPOINT ["/opt/entrypoint.sh"]
