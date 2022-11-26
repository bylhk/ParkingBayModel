docker build . -t parking:latest
docker run --rm -p 8501:8501 parking:latest