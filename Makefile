.PHONY: generate
generate:
	$(MAKE) -C api generate

.PHONY: build
build:
	go build

.PHONY: clean
clean:
	rm -rf bin
	rm -rf api/proto/openapi
	find ./api/proto -name '*.pb.go' | xargs rm
	find ./api/proto -name '*.pb.validate.go' | xargs rm
	find ./api/proto -name '*.pb.gw.go' | xargs rm
