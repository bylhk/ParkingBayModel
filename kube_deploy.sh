eval $(minikube docker-env)
docker build . -t pz/parking:latest
minikube image load pz/parking:latest
minikube kubectl -- apply -f kube_deploy.yaml
echo $(minikube service parking --url)