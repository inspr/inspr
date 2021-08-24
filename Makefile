# vim: set foldmarker={,} foldlevel=0 foldmethod=marker:

-include ./.env

VERSION ?= $(shell git describe --long --always --dirty)
PROFILE ?= inspr-stack
K8S_NAMESPACE ?= ${VERSION}
GOFLAGS ?= 
RELEASE_NAME ?= ""

VALUES ?= stack-overwrites.yaml
INSPRD_VALUES ?= insprd-overwrites.yaml
UIDP_VALUES ?= uidp-overwrites.yaml
export



help:
	@python ./scripts/makefile_help.py Makefile $C

# golang {

## downloads dependencies
go/download:
	go mod download

## builds all binaries to the bin directory
go/build:
	mkdir -p build/bin
	go build ${GOFLAGS} -o build/bin ./...

## runs all tests on the repo
go/test:
	go test ./...

## runs all tests tagged integration
go/test/integration:
	go test ./... -tags=integration

## lints the repo with goling and staticcheck
go/lint:
	staticcheck ./...
	golint ./...

## gets tools necessary for development
go/init:
	go get -u golang.org/x/lint/golint
	go get -u github.com/ptcar2009/ptcwatcher/cmd/ptcwatcher


## runs coverage and exports coverage profile
go/coverage:
	bash ./.github/scripts/unittest.sh

## watches the files and lints on changes
go/lint/watch:
	ptcwatcher 'make go/lint' -w ./pkg -w ./cmd

## watches the files and builds on changes
go/build/watch:
	ptcwatcher 'make go/build' -w ./pkg -w ./cmd

## watches the files and tests on changes
go/test/watch:
	ptcwatcher 'make go/test' -w ./pkg -w ./cmd
# }

# CLI {

# insprctl {
## builds insprctl to the bin directory
cli/insprctl/build:
	go build -o bin ./cmd/insprctl

## installs insprctl to $GOPATH/bin
cli/insprctl/install:
	go install ./cmd/insprctl
# }

# inprov {
## builds inprov to the bin directori
cli/inprov/build:
	go build -o bin ./cmd/uid_provider/inprov

## installs inprov to $GOPATH/bin
cli/inprov/install:
	go install ./cmd/uid_provider/inprov
# }

# all {
## builds all CLI tools to the bin directory
cli/build: cli/insprctl/build cli/inprov/build

## installs all CLI tools to $GOPATH/bin
cli/install: cli/insprctl/install cli/inprov/install
# }
# }

# CI {

## runs all scripts regarding CI, including linting, coverage and initialization
ci/all:  ci/init ci/lint ci/test ci/coverage

## initializes the environment for CI
ci/init: go/init helm/init semgrep/init

## lints the go src, helm templates and runs semgrep jobs
ci/lint: go/lint semgrep/run helm/lint

## runs all tests regarding golang
ci/test: go/test go/test/integration

## runs coverage on the repo and exports the profile
ci/coverage: go/coverage

## builds the CLI to all platforms and syncs to the repo
ci/release: ci/cli/push

# CLI {
## builds the CLI to all platforms and architectures
ci/cli/build:
	bash ./.github/scripts/buildcli.sh

## pushes the built binaries to the CI repo
ci/cli/push: ci/cli/build
	bash ./.github/scripts/pushcli.sh
# }
# }

# helm {

# uidp {
## packages the UIDP helm chart using the UIDP overrides file, which by default is uidp-overwrites.yaml
helm/uidp/package:
	helm package ./build/inspr-stack/subcharts/uidp -o charts -f ${UIDP_VALUES} -n ${K8S_NAMESPACE}

## lints the UIDP helm chart using the UIDP overrides file.
helm/uidp/lint:
	helm lint ./build/inspr-stack/subcharts/uidp -o charts -f ${UIDP_VALUES} -n ${K8S_NAMESPACE}

## runs the UIDP helm chart tests using the UIDP overrides file,
helm/uidp/test:
	helm test ./build/inspr-stack/subcharts/uidp -o charts -f ${UIDP_VALUES} -n ${K8S_NAMESPACE}

## installs the uidp helm chart to the K8S_NAMESPACE using the uidp overrides file.
helm/uidp/install:
	helm install ./build/inspr-stack/subcharts/uidp -o charts -f ${UIDP_VALUES} -n ${K8S_NAMESPACE}
# }

# insprd {
## packages the INSPRD helm chart using the INSPRD overrides file, which by default is insprd-overwrites.yaml
helm/insprd/package:
	helm package ./build/inspr-stack/subcharts/insprd -o charts -f ${INSPRD_VALUES} -n ${K8S_NAMESPACE}

## lints the INSPRD helm chart using the INSPRD overrides file.
helm/insprd/lint:
	helm lint ./build/inspr-stack/subcharts/insprd -o charts -f ${INSPRD_VALUES} -n ${K8S_NAMESPACE}

## runs the INSPRD helm chart tests using the INSPRD overrides file,
helm/insprd/test:
	helm test ./build/inspr-stack/subcharts/insprd -o charts -f ${INSPRD_VALUES} -n ${K8S_NAMESPACE}

## installs the insprd helm chart to the K8S_NAMESPACE using the insprd overrides file.
helm/insprd/install:
	helm install ./build/inspr-stack/subcharts/insprd -o charts -f ${INSPRD_VALUES} -n ${K8S_NAMESPACE}
# }

# stack {
## packages the INSPR-STACK helm chart using the INSPR-STACK overrides file, which by default is stack-overwrites.yaml
helm/package:
	helm package ./build/inspr-stack -o charts -f ${VALUES} -n ${K8S_NAMESPACE}

## lints the INSPR-STACK helm chart using the INSPR-STACK overrides file.
helm/lint:
	helm lint ./build/inspr-stack -o charts -f ${VALUES} -n ${K8S_NAMESPACE}

## runs the INSPR-STACK helm chart tests using the INSPR-STACK overrides file,
helm/test:
	helm test ./build/inspr-stack -o charts -f ${VALUES} -n ${K8S_NAMESPACE}

## installs the inspr-stack helm chart to the K8S_NAMESPACE using the inspr-stack overrides file.
helm/install:
	helm install ./build/inspr-stack -o charts -f ${VALUES} -n ${K8S_NAMESPACE}
# }
# }

# Skaffold {

## runs skaffold build with the PROFILE profile and outputs the image to OUTPUT_FILE if defined.
skaffold/build:
ifdef OUTPUT_FILE
	skaffold build -p ${PROFILE} -o ${OUTPUT_FILE}
else
	skaffold build -p ${PROFILE}
endif

## runs skaffold run with the PROFILE profile on the K8S_NAMESPACE namespace.
skaffold/run:
	skaffold run -p ${PROFILE} -n ${K8S_NAMESPACE}

skaffold/dev:
	skaffold dev -p ${PROFILE} -n ${K8S_NAMESPACE}

skaffold/delete:
	skaffold delete -p ${PROFILE} -n ${K8S_NAMESPACE}
# }

# semgrep {
## downloads sempgrep and installs it using python3
semgrep/init:
	python3 -m pip install semgrep

## runs the desired test suites for semgrep
semgrep/run:
	semgrep --config "p/trailofbits"
# }

