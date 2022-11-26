FROM python:3.8.1

WORKDIR /parking

RUN apt-get update -y

COPY requirements.txt requirements.txt

RUN apt-get update -y \ 
 && python3 -m pip install --no-cache-dir -U pip \
 && python3 -m pip install --no-cache-dir -r requirements.txt

COPY app/ ./

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
