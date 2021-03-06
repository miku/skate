SHELL := /bin/bash
TARGETS := skate-ref-to-release skate-derive-key skate-cluster skate-biblioref skate-cluster-stats skate-verify skate-to-doi skate-bref-id skate-from-unstructured
PKGNAME := skate

.PHONY: test
test:
	go test -cover -v ./...

.PHONY: generate
generate:
	go generate

.PHONY: all
all: generate $(TARGETS)

%: cmd/%/main.go
	go build -o $@ $<

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


