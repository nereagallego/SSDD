kubectl delete -f statefulset_go.yaml
kubectl delete pod raft-0
kubectl delete pod raft-1
kubectl delete pod raft-2
kubectl create -f statefulset_go.yaml
