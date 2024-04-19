FROM bitnami/spark:latest
WORKDIR /app
COPY . /app
##RUN pip --no-cache-dir install -r requirements.txt
RUN pip install -r requirements.txt
CMD ["python", "app.py"]