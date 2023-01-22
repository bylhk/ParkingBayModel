docker build . -t pz/parking:latest
# kubectl apply -f deployment.yaml
docker run --rm -p 8501:8501 pz/parking:latest
