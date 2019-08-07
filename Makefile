PACKAGE  = github.com/xbcsmith/autoeureka
BINARY    = bin/autoeureka
COMMIT  ?= $(shell git rev-parse --short=16 HEAD)
gitversion := $(shell git describe --tags --always --dirty --match=v* 2> /dev/null || \
			cat $(CURDIR)/.version 2> /dev/null || echo 0.1.0-0)
VERSION ?= $(gitversion)

TOOLS    = $(CURDIR)/tools
PKGS     = $(or $(PKG),$(shell $(GO) list ./... | grep -v "^$(PACKAGE)/vendor/"))
TESTPKGS = $(shell $(GO) list -f '{{ if or .TestGoFiles .XTestGoFiles }}{{ .ImportPath }}{{ end }}' $(PKGS))
GOLDFLAGS = "-X $(PACKAGE)/config.Version=$(VERSION) -X $(PACKAGE)/config.Commit=$(COMMIT)"

export GO111MODULE=on

# Allow tags to be set on command-line, but don't set them
# by default
override TAGS := $(and $(TAGS),-tags $(TAGS))

GO      = go
GOBUILD = CGO_ENABLED=0 go build -v
GODOC   = godoc
GOFMT   = gofmt
GOGENERATE = go generate
TIMEOUT = 15


V = 0
Q = $(if $(filter 1,$V),,@)
M = $(shell printf "\033[34;1m▶\033[0m")

.PHONY: all
all: fmt lint test $(BINARY) $(BINARY)-arm64 $(BINARY)-ppc64le $(BINARY)-darwin

.PHONY: linux
linux: fmt lint test $(BINARY)

.PHONY: darwin
darwin: fmt lint test $(BINARY)-darwin



SOURCES = $(shell find -name vendor -prune -o -name \*.go -print)

$(BINARY): $(SOURCES); $(info $(M) building linux executable…) @ ## Build program binary
	$Q GOOS=linux GOARCH=amd64 $(GOBUILD) $(TAGS) -ldflags $(GOLDFLAGS) -o $@ .

$(BINARY)-arm64: $(SOURCES); $(info $(M) building arm64 executable…) @ ## Build program binary for arm64
	$Q GOOS=linux GOARCH=arm64 $(GOBUILD) $(TAGS) -ldflags $(GOLDFLAGS) -o $@ .

$(BINARY)-ppc64le: $(SOURCES); $(info $(M) building ppc64le executable…) @ ## Build program binary for ppc64le
	$Q GOOS=linux GOARCH=ppc64le $(GOBUILD) $(TAGS) -ldflags $(GOLDFLAGS) -o $@ .

$(BINARY)-darwin: $(SOURCES); $(info $(M) building darwin executable…) @ ## Build program binary
	$Q GOOS=darwin GOARCH=amd64 $(GOBUILD) $(TAGS) -ldflags $(GOLDFLAGS) -o $@ .



# Tools

GOLINT = $(TOOLS)/golint
$(GOLINT): ; $(info $(M) building golint…)
	$Q go build -o $@ golang.org/x/lint/golint

GOCOVMERGE = $(TOOLS)/gocovmerge
$(GOCOVMERGE): ; $(info $(M) building gocovmerge…)
	$Q go build -o $@ github.com/wadey/gocovmerge

GOCOV = $(TOOLS)/gocov
$(GOCOV): ; $(info $(M) building gocov…)
	$Q go build -o $@ github.com/axw/gocov/gocov

GOCOVXML = $(TOOLS)/gocov-xml
$(GOCOVXML): ; $(info $(M) building gocov-xml…)
	$Q go build -o $@ github.com/AlekSi/gocov-xml

GO2XUNIT = $(TOOLS)/go2xunit
$(GO2XUNIT): ; $(info $(M) building go2xunit…)
	$Q go build -o $@ github.com/tebeka/go2xunit

GOBINDATA = $(TOOLS)/go-bindata
$(GOBINDATA): ; $(info $(M) building go-bindata…)
	@mkdir -p $(TOOLS)
	$Q go build -o $@ github.com/shuLhan/go-bindata/cmd/go-bindata

GOVERSIONINFO = $(TOOLS)/goversioninfo
$(GOVERSIONINFO): ; $(info $(M) building goversioninfo…)
	@mkdir -p $(TOOLS)
	$Q go build -o $@ github.com/josephspurrier/goversioninfo/cmd/goversioninfo


$(TOOLS)/protoc-gen-go: ; $(info $(M) building protoc-gen-go…)
	@mkdir -p $(TOOLS)
	$Q go build -o $@ github.com/golang/protobuf/protoc-gen-go

.PHONY: lint
lint: $(GOLINT) ; $(info $(M) running golint…) @ ## Run golint change ret=1 to make lint required
	$Q ret=0 && for pkg in $(PKGS); do \
		test -z "$$($(GOLINT) $$pkg | tee /dev/stderr)" || ret=0 ; \
	 done ; exit $$ret

.PHONY: fmt
fmt: ; $(info $(M) running gofmt…) @ ## Run gofmt on all source files
	@ret=0 && for d in $$($(GO) list -f '{{.Dir}}' ./... | grep -v /vendor/); do \
		$(GOFMT) -l -w $$d/*.go || ret=$$? ; \
	 done ; exit $$ret

.PHONY: test
test: ; $(info $(M) running tests…) @
	$Q go test -v

.PHONY: clean
clean: ; $(info $(M) cleaning…)	@ ## Cleanup everything
	@rm -rf bin tools vendor


.PHONY: help
help:
	@grep -E '^[ a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

.PHONY: version
version:
	@echo $(VERSION)
