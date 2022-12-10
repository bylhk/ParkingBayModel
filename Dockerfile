FROM apache/spark-py:3.3.1

WORKDIR /parking
COPY requirements.txt requirements.txt

USER root

RUN apt-get update -y \ 
 && python3 -m pip install --no-cache-dir -U pip \
 && python3 -m pip install --no-cache-dir -r requirements.txt \
 && rm -rf /var/lib/apt/lists/*

COPY app/ ./

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
