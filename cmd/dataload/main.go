package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"github.com/pkg/errors"
	"github.com/schollz/progressbar"
	"google.golang.org/grpc"
)

const (
	urlPrefix = "/api/v2"
	debug     = false
)

func main() {
	path := flag.String("data", "./api-data/data",
		"path to the directory containing the PokeAPI data")
	flag.Parse()

	conn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connecto to dgraph: %v", err)
	}
	defer conn.Close()
	client := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	if err := setSchema(client); err != nil {
		log.Fatalf("could not fetch schema: %v", err)
	}

	if err := load(client, filepath.Join(*path, urlPrefix)); err != nil {
		log.Fatal(err)
	}
}

func setSchema(client *dgo.Dgraph) error {
	return client.Alter(context.Background(), &api.Operation{
		Schema: `
			url: string @index(exact) .
			name: string @index(term) .
		`,
	})
}

func load(client *dgo.Dgraph, path string) error {
	fis, err := ioutil.ReadDir(path)
	if err != nil {

		return errors.Wrapf(err, "could not list files in %s", path)
	}

	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}
		typename := fi.Name()
		if err := loadType(client, typename, filepath.Join(path, typename)); err != nil {
			return errors.Wrapf(err, "could not load type %s", typename)
		}
	}
	return nil
}

func loadType(client *dgo.Dgraph, typename, path string) error {
	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return errors.Wrapf(err, "could not read files in %s", path)
	}

	fmt.Printf("\nloading %s (~%d items)\n", typename, len(fis))
	bar := progressbar.New(len(fis))

	for _, fi := range fis {
		if fi.IsDir() {
			err := loadFile(client, typename, filepath.Join(path, fi.Name()))
			if err != nil {
				return errors.Wrapf(err, "could not load file %s", fi.Name())
			}
		}
		bar.Add(1)
	}
	return nil
}

func loadFile(client *dgo.Dgraph, typename, path string) error {
	var data map[string]interface{}
	if err := parseJSON(filepath.Join(path, "index.json"), &data); err != nil {
		return errors.Wrapf(err, "could not parse JSON")
	}

	// The top level objects don't have a URL, we need to add it.
	data["url"] = path[strings.Index(path, urlPrefix):] + "/"
	data["dgraph.type"] = typename

	preprocess(data, path)

	if err := mutate(client, data); err != nil {
		return errors.Wrapf(err, "could not mutate")
	}
	return nil
}

func parseJSON(path string, dest interface{}) error {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return errors.Wrapf(err, "could not open %s", path)
	}
	if err := json.Unmarshal(b, dest); err != nil {
		return errors.Wrapf(err, "could not parse JSON in %s", path)
	}
	return nil
}

var uids = uidCache{blanks: make(map[string]string), uids: make(map[string]string)}

type uidCache struct {
	uids        map[string]string
	blanks      map[string]string
	latestBlank int
	sync.Mutex
}

func (c *uidCache) setBlank(blank, uid string) {
	c.Lock()
	defer c.Unlock()

	logf("setting %s to %s", blank, uid)

	// find what URL was given for this blank
	var url string
	for u, b := range c.blanks {
		if b != blank {
			continue
		}
		url = u
		break
	}

	if url == "" {
		// this was an anonymous one, nothing left to do
		return
	}
	delete(c.blanks, url)

	logf("new uid for %s is %s", url, uid)

	old, ok := c.uids[url]
	if ok && uid != old {
		log.Fatalf("uid already assigned for %s: was %s got %s", url, old, uid)
	}

	c.uids[url] = uid
}

func (c *uidCache) anonymous() string {
	c.Lock()
	defer c.Unlock()
	c.latestBlank++
	return fmt.Sprintf("_:%d", c.latestBlank)
}

func (c *uidCache) get(url string) string {
	c.Lock()
	defer c.Unlock()

	// first check whether it's been assigned
	uid, ok := c.uids[url]
	if ok {
		logf("got uid for %s from cache: %s", url, uid)
		return uid
	}

	// next check whether it's been requested and a blank has been given
	uid, ok = c.blanks[url]
	if ok {
		logf("got uid for %s from blank cache: %s", url, uid)
		return uid
	}

	// last create a new blank uid and return it
	c.latestBlank++
	uid = fmt.Sprintf("_:%d", c.latestBlank)
	c.blanks[url] = uid
	logf("new blank uid for %s is %s", url, uid)
	return uid
}

func preprocess(data interface{}, context string) {
	switch t := data.(type) {
	case map[string]interface{}:
		// names are slightly complicated to handle, so dropping them for now
		delete(t, "names")
		if url, ok := t["url"]; ok {
			t["uid"] = uids.get(url.(string))
		}
		if _, ok := t["uid"]; !ok {
			t["uid"] = uids.anonymous()
		}
		for k, v := range t {
			preprocess(v, fmt.Sprintf("%s/%s", context, k))
		}
	case []interface{}:
		for i, v := range t {
			preprocess(v, fmt.Sprintf("%s[%d]", i))
		}
	case string, float64, bool, nil:
		// nothing
	default:
		logf("unknown type %T", data)
	}
}

func mutate(client *dgo.Dgraph, data interface{}) error {
	b, err := json.Marshal(data)
	if err != nil {
		return errors.Wrapf(err, "could not marshal")
	}
	logf("sending mutation: %s", b)
	res, err := client.NewTxn().Mutate(context.Background(), &api.Mutation{
		SetJson:   b,
		CommitNow: true,
	})
	if err != nil {
		return errors.Wrapf(err, "could not mutate")
	}
	logf("%s", res.Json)

	for blank, uid := range res.Uids {
		logf("%s -> %s", blank, uid)
		uids.setBlank("_:"+blank, uid)
	}

	return nil
}

func logf(format string, args ...interface{}) {
	if debug {
		log.Printf(format, args...)
	}
}
