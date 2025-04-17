## To build
`kustomize build .`

## to check what the difference with the cluster resources is
`kustomize build .|kubectl diff -f -`

## to apply to the cluster
`kustomize build .|kubectl apply -f -`