# Copyright 2014 The Cockroach Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License. See the AUTHORS file
# for names of contributors.

# Cockroach build rules.
GO ?= go
# Allow setting of go build flags from the command line.
GOFLAGS :=
# Set to 1 to use static linking for all builds (including tests).
STATIC :=

PKG := kv ycsb tpcc tpch
GOPKG := $(patsubst %,./%,$(PKG))

ifeq ($(STATIC),1)
LDFLAGS += -extldflags "-static"
endif

.PHONY: all
all: build test check

.PHONY: test
test:
	$(GO) test -v -i $(GOPKG)
	$(GO) test -v $(GOPKG)

.PHONY: deps
deps:
	GOBIN=$(abspath  bin) $(GO) install -v \
		./vendor/github.com/golang/lint/golint \
		./vendor/github.com/kisielk/errcheck \
		./vendor/golang.org/x/tools/cmd/goimports


.PHONY: build
build: deps $(PKG)

.PHONY: $(PKG)
$(PKG):
	$(GO) build -tags '$(TAGS)' $(GOFLAGS) -ldflags '$(LDFLAGS)' -v -i -o $@/$@ ./$@

.PHONY: check
check:
	@echo "checking for tabs in shell scripts"
	@! git grep -F '	' -- ':!vendor' '*.sh'
	@echo "checking for \"path\" imports"
	@! git grep -F '"path"' -- ':!vendor' '*.go'
	@echo "errcheck"
	@./bin/errcheck -exclude errcheck_excludes.txt $(GOPKG)
	@echo "vet"
	@! go tool vet $(PKG) 2>&1 | \
	  grep -vE '^vet: cannot process directory .git'
	@echo "vet --shadow"
	@! go tool vet --shadow $(PKG) 2>&1 | \
	  grep -vE '(declaration of err shadows|^vet: cannot process directory \.git)'
	@echo "golint"
	@! ./bin/golint $(PKG) | grep -vE '(\.pb\.go|_string)'
	@echo "gofmt (simplify)"
	@! gofmt -s -d -l $(PKG) 2>&1 | grep -vE '^\.git/'
	@echo "goimports"
	@! ./bin/goimports -l $(PKG) | grep -vF 'No Exceptions'
