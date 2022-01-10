kubectl delete -f statefulset_go.yaml
kubectl delete pvc www
kubectl delete pvc www-raft-0
kubectl delete pvc www-raft-1
kubectl delete pvc www-raft-2
#kubectl delete statefulset raft
#kubectl delete service almacen
kubectl delete pod raft-0
kubectl delete pod raft-1
kubectl delete pod raft-2
kubectl delete configmap cm-go
kubectl create configmap cm-go --from-file=./fichero
kubectl create -f statefulset_go.yaml
#kubectl apply -f statefulset_go.yaml
