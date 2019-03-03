package main

import (
	"bitbucket.org/lexbond/stream-url/url"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"log"
	"sync"
)

const (
	address = "localhost:50052"
	countRoutines = 1000
)

func GetRandomDataStream(client url.UrlClient) {

	stream, err := client.GetRandomDataStream(context.Background(), &url.Empty{})

	if err != nil {
		fmt.Println("Err: ", err)
	}
	for {
		text, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Err: ", err)
			break
		}
		fmt.Println("Response: ", text)
	}
}

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatal("cannot connect to address ", err)
	} else {

		defer conn.Close()
		wg := sync.WaitGroup{}

		for i := 0; i < countRoutines; i++ {

			wg.Add(1)

			go func(wGroup *sync.WaitGroup, j int) {
				defer wGroup.Done()
				defer func(i int) {

					fmt.Println(fmt.Sprintf("stop consumer # %v", i))
				}(j)

				fmt.Println(fmt.Sprintf("run consumer # %v", j))

				client := url.NewUrlClient(conn)

				GetRandomDataStream(client)
			}(&wg, i)
		}
		wg.Wait()
	}
}