package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	client := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	predicates, err := fetchPredicates(client)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("loaded %d predicates", len(predicates))

	types, err := fetchTypes(client)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("loaded %d types", len(types))

	for _, typename := range types {
		if err := findFields(client, typename, predicates); err != nil {
			log.Fatal(err)
		}
	}
}

type Predicate struct {
	Predicate string
	Type      string
	List      bool
}

func (p Predicate) typedef() string {
	if !p.List {
		return p.Type
	}
	return fmt.Sprintf("[%s]", p.Type)
}

func fetchPredicates(client *dgo.Dgraph) (map[string]Predicate, error) {
	res, err := client.NewReadOnlyTxn().Query(context.Background(), "schema{}")
	if err != nil {
		return nil, errors.Wrapf(err, "could not fetch schema")
	}

	var data struct{ Schema []Predicate }
	if err := json.Unmarshal(res.Json, &data); err != nil {
		return nil, errors.Wrapf(err, "could not parse schema")
	}

	preds := make(map[string]Predicate)
	for _, p := range data.Schema {
		if !strings.HasPrefix(p.Predicate, "dgraph.") {
			preds[p.Predicate] = p
		}
	}
	return preds, nil
}

func fetchTypes(client *dgo.Dgraph) ([]string, error) {
	res, err := client.NewReadOnlyTxn().Query(context.Background(),
		"{ types(func: has(dgraph.type)) @groupby(dgraph.type) {} }")
	if err != nil {
		return nil, errors.Wrapf(err, "could not fetch types")
	}

	var data struct {
		Types []struct {
			GroupBy []struct {
				Type string `json:"dgraph.type"`
			} `json:"@groupby"`
		}
	}
	if err := json.Unmarshal(res.Json, &data); err != nil {
		return nil, errors.Wrapf(err, "could not parse types")
	}

	var types []string
	for _, t := range data.Types[0].GroupBy {
		types = append(types, t.Type)
	}
	sort.Strings(types)
	return types, nil
}

func findFields(client *dgo.Dgraph, typename string, preds map[string]Predicate) error {
	var predNames []string
	for name, pred := range preds {
		if pred.Type == "uid" {
			// otherwise the predicate won't appear int he result, as there's no fields requested.
			predNames = append(predNames, name+"{uid}")
		} else {
			predNames = append(predNames, name)
		}
	}
	sort.Strings(predNames)
	query := fmt.Sprintf("{ values(func: type(%s)) { %s } }",
		typename, strings.Join(predNames, "\n"))

	res, err := client.NewReadOnlyTxn().Query(context.Background(), query)
	if err != nil {
		return errors.Wrapf(err, "could not fetch types")
	}
	var data struct{ Values []map[string]interface{} }
	if err := json.Unmarshal(res.Json, &data); err != nil {
		return errors.Wrapf(err, "could not parse types")
	}

	fields := make(map[string]bool)
	for _, value := range data.Values {
		for field := range value {
			fields[field] = true
		}
	}

	fmt.Printf("type %s {\n", typename)
	for k := range fields {
		fmt.Printf("\t%s: %s\n", k, preds[k].typedef())
	}
	fmt.Printf("}\n\n")
	return nil
}
