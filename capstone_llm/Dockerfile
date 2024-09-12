FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.1-hadoop-3.3.6-v1

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

#TODO add your project code and dependencies to the image
COPY requirements.txt .
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install -r requirements.txt

COPY . .

RUN python3 -m pip install -e .
ENTRYPOINT [ "python3" ]
CMD [ "-m", "capstonellm.tasks.clean" , "-e", "prod", "-t", "dbt"]
