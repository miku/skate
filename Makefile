SHELL := /bin/bash
TARGETS := skate-ref-to-release skate-sorted-keys skate-cluster
PKGNAME := skate

COMMIT := $(shell git rev-parse --short HEAD)
BUILDTIME := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

GOLDFLAGS += -X main.Commit=$(COMMIT)
GOLDFLAGS += -X main.Buildtime=$(BUILDTIME)
GOFLAGS = -ldflags "$(GOLDFLAGS)"


.PHONY: all
all: $(TARGETS)

%: cmd/%/main.go
	go build -o $@ -ldflags "$(GOLDFLAGS)" $<

.PHONY: clean
clean:
	rm -f $(TARGETS)
	rm -f $(PKGNAME)_*.deb
	rm -f $(PKGNAME)*.rpm
	rm -rf packaging/debian/$(PKGNAME)/usr

.PHONY: deb
deb: all
	mkdir -p packaging/debian/$(PKGNAME)/usr/local/bin
	cp $(TARGETS) packaging/debian/$(PKGNAME)/usr/local/bin
	cd packaging/debian && fakeroot dpkg-deb --build $(PKGNAME) .
	mv packaging/debian/$(PKGNAME)_*.deb .


