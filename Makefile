
PROJECT=hubble-rbac-controller
ORG?=lunarway
REG?=quay.io
TAG?=latest
NAMESPACE=default
SHELL=/bin/bash
COMPILE_TARGET=./build/_output/bin/$(PROJECT)
GOOS=darwin
GOARCH=amd64

.PHONY: code/generate
code/generate:
	operator-sdk generate k8s

.PHONY: code/compile
code/compile:
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -o=$(COMPILE_TARGET) ./cmd/manager

.PHONY: test/unit
test/integration:
	go test ./pkg/...

.PHONY: test/integration
test/integration:
	go test -tags integration ./pkg/...

.PHONY: image/build
image/build: code/compile
	operator-sdk build ${REG}/${ORG}/${PROJECT}:${TAG}

.PHONY: image/push
image/push:
	docker push ${REG}/${ORG}/${PROJECT}:${TAG}

.PHONY: release
release:
	sed -i "" 's|${REG}/${ORG}/${PROJECT}.*|${REG}/${ORG}/${PROJECT}:${TAG}|g' deploy/operator.yaml
	git add deploy/operator.yaml
	git commit -m"Release ${TAG}"
	git tag ${TAG}
	git push
	git push --tags