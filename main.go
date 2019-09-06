package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

const pokeAPI = "https://pokeapi.co/api/v2/"

func main() {
	conn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	if err := client.Alter(context.Background(), &api.Operation{
		Schema: `
			url: string @index(exact) .
			name: string @index(term) .
		`,
	}); err != nil {
		log.Fatal(err)
	}

	var urls map[string]string
	if err := get(pokeAPI, &urls); err != nil {
		log.Fatal(err)
	}

	for kind, url := range urls {
		var count struct{ Count int }
		if err := get(url, &count); err != nil {
			log.Fatal(err)
		}
		fmt.Println(kind, count.Count)

		var data struct {
			Results []struct {
				Name string
				URL  string
			}
		}
		if err := get(fmt.Sprintf("%s?limit=%d", url, count.Count), &data); err != nil {
			log.Fatal(err)
		}

		for _, res := range data.Results {
			err := mutate(client, url, struct {
				URL  string `json:"url"`
				Name string `json:"name"`
				Type string `json:"dgraph.type"`
			}{
				res.URL,
				res.Name,
				kind,
			})
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func get(url string, dest interface{}) error {
	log.Printf("fetching %s", url)
	res, err := http.Get(url)
	if err != nil {
		return errors.Wrapf(err, "could not fetch %s", url)
	}
	if res.StatusCode != http.StatusOK {
		return errors.Errorf("%s returned status code %s", url, res.Status)
	}
	defer res.Body.Close()

	if err := json.NewDecoder(res.Body).Decode(dest); err != nil {
		return errors.Wrapf(err, "could not decode payload from %s", url)
	}
	return nil
}

func mutate(client *dgo.Dgraph, url string, data interface{}) error {
	b, err := json.Marshal(data)
	if err != nil {
		return errors.Wrapf(err, "could not marshal")
	}
	res, err := client.NewTxn().Do(context.Background(), &api.Request{
		Query: fmt.Sprintf(`
		{
			v as var(func: eq(url, %q))
		}
		`, url),
		Mutations: []*api.Mutation{{
			Cond:    "@if(eq(len(v), 0))",
			SetJson: b,
		}},
		CommitNow: true,
	})
	if err != nil {
		return errors.Wrapf(err, "could not mutate")
	}
	log.Println(res)
	return nil
}
