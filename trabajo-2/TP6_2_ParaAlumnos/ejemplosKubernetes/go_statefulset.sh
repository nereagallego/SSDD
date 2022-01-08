kubectl delete statefulset r
kubectl delete service almacen
kubectl delete configmap cm-elixir
kubectl create configmap cm-elixir --from-file=./programa
kubectl create -f statefulset_elixir.yaml
