FROM apache/spark:3.5.3-python3
USER root
RUN pip install --no-cache-dir requests==2.32.3 scikit-learn==1.5.2 numpy==2.1.3
COPY jobs /opt/spark/jobs
USER 185
ENTRYPOINT ["/opt/entrypoint.sh"]
