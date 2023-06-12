package main

import (
	"context"
	"io"
	"log"
	"os"
	"sigs.k8s.io/yaml"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	databasev1 "github.com/lujiajing1126/banyandb-billy/api/proto/banyandb/database/v1"
)

func main() {
	f, err := os.Open("./scripts/topn.yaml")
	if err != nil {
		log.Fatal("fail to open file")
	}
	yamlContent, err := io.ReadAll(f)
	if err != nil {
		log.Fatal("fail to read file")
	}
	jsonContenxt, err := yaml.YAMLToJSON(yamlContent)
	if err != nil {
		log.Fatal("fail to convert yaml to json")
	}
	var topNAggr databasev1.TopNAggregation
	err = protojson.Unmarshal(jsonContenxt, &topNAggr)
	if err != nil {
		log.Fatal("fail to unmarshal topN schema")
	}
	conn, err := grpc.Dial("localhost:17912", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("cannot establish a connection to the target: %s", err)
	}
	defer conn.Close()
	registryClient := databasev1.NewTopNAggregationRegistryServiceClient(conn)
	_, err = registryClient.Create(context.Background(), &databasev1.TopNAggregationRegistryServiceCreateRequest{
		TopNAggregation: &topNAggr,
	})
	if err != nil {
		log.Fatalf("fail to create TopN: %s", err)
	}
	log.Println("TopNAggregation created")
}
