package main

import (
	"context"
	"io"
	"log"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gopkg.in/yaml.v3"

	databasev1 "github.com/lujiajing1126/banyandb-billy/api/proto/banyandb/database/v1"
)

func main() {
	f, err := os.Open("./topn.yaml")
	if err != nil {
		log.Fatal("fail to open file")
	}
	content, err := io.ReadAll(f)
	if err != nil {
		log.Fatal("fail to read file")
	}
	var topNAggr databasev1.TopNAggregation
	err = yaml.Unmarshal(content, &topNAggr)
	if err != nil {
		log.Fatal("fail to unmarshal topN schema")
	}
	conn, err := grpc.Dial("localhost:17912", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("cannot establish a connection to the target: %s", err)
	}
	defer conn.Close()
	registryClient := databasev1.NewTopNAggregationRegistryServiceClient(conn)
	resp, err := registryClient.Create(context.Background(), &databasev1.TopNAggregationRegistryServiceCreateRequest{
		TopNAggregation: &topNAggr,
	})
	if err != nil {
		log.Fatalf("fail to create TopN: %s", err)
	}
	log.Println("TopNAggregation created")
}
