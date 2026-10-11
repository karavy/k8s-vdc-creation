# Local development stack: operator + in-cluster mock API + one safe smoke-test CR.
# This intentionally supports only the verified local Kind context.
allow_k8s_contexts('kind-er-operator')

# Build the operator image with the sibling local Go modules required by go.mod.
custom_build(
    'controller:tilt',
    'docker build --build-context=api-engine=../api-engine --build-context=terraform-engine=../terraform-engine -f Dockerfile.tilt -t $EXPECTED_REF .',
    deps=[
        'Dockerfile.tilt', 'go.mod', 'go.sum', 'api', 'cmd', 'internal',
        '../api-engine', '../terraform-engine',
    ],
)

# Build the standalone mock HTTP API image from local source.
docker_build(
    'k8s-vdc-creation-mock-api:tilt',
    '.',
    dockerfile='Dockerfile.mock-api',
)

# Kustomize deploys CRDs, RBAC, operator, ClusterIP mock API, and a single
# POST /echo smoke test. Tilt streams logs and reports pod/CR status.
k8s_yaml(kustomize('config/tilt'))

# Link each workload to its locally built image in the Tilt UI.
k8s_resource('k8s-vdc-creation-controller-manager')
k8s_resource('mock-api', port_forwards=['8088:8080'])
