.PHONY: generate
generate:
	$(MAKE) -C api generate

.PHONY: build
build:
	go build