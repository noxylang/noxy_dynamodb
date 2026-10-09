// noxy_dynamodb — DynamoDB for Noxy, packaged as a process extension
// (kind = "process" in noxy_ext.toml).
//
// The executable speaks noxy-plugin/1 over stdin/stdout through the Go SDK;
// noxy_ext.toml declares the exports and noxy_dynamodb.nx is the typed
// wrapper users import. One process holds every client behind an integer
// handle minted by dynamodb_connect; concurrency = "concurrent" lets calls
// on different (or the same) handles interleave.
package main

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/noxylang/noxy/sdk/noxyplugin"
)

func main() {
	p := noxyplugin.New()
	for name, h := range newService(connectAWS).handlers() {
		p.Handle(name, h)
	}
	p.Main()
}

// dynamoAPI is the slice of *dynamodb.Client the handlers use; tests plug in
// a fake.
type dynamoAPI interface {
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// connectOptions are the keys dynamodb_connect accepts in its options map.
type connectOptions struct {
	Region   string // "region": overrides AWS_REGION / the shared config
	Endpoint string // "endpoint": e.g. http://localhost:8000 for DynamoDB Local
	Profile  string // "profile": a named profile from the shared config files
}

// dialer turns parsed options into a client; connectAWS in production.
type dialer func(context.Context, connectOptions) (dynamoAPI, error)

// service is the process-wide state: the handle table and the dialer.
type service struct {
	dial    dialer
	mu      sync.Mutex
	next    int64
	clients map[int64]dynamoAPI
}

func newService(dial dialer) *service {
	return &service{dial: dial, clients: map[int64]dynamoAPI{}}
}

// handlers maps every [[export]] of noxy_ext.toml to its handler.
// dynamodb_query_page takes six arguments, one past the SDK's Func5, so it
// is an untyped Handler that checks its own arity and types.
func (s *service) handlers() map[string]noxyplugin.Handler {
	return map[string]noxyplugin.Handler{
		"dynamodb_connect":     noxyplugin.Func1(s.connect),
		"dynamodb_put_item":    noxyplugin.Func3(s.putItem),
		"dynamodb_get_item":    noxyplugin.Func3(s.getItem),
		"dynamodb_update_item": noxyplugin.Func5(s.updateItem),
		"dynamodb_delete_item": noxyplugin.Func3(s.deleteItem),
		"dynamodb_scan":        noxyplugin.Func3(s.scan),
		"dynamodb_scan_page":   noxyplugin.Func4(s.scanPage),
		"dynamodb_query":       noxyplugin.Func5(s.query),
		"dynamodb_query_page":  s.queryPageHandler,
		"dynamodb_close":       noxyplugin.Func1(s.closeClient),
	}
}

// connectAWS is the production dialer: the default credential and config
// chain (environment, shared config, instance/Lambda role). The region comes
// from the "region" option, else AWS_REGION / the shared config, else
// us-east-1 — the default the JSON plugin always used.
func connectAWS(ctx context.Context, o connectOptions) (dynamoAPI, error) {
	var loadOpts []func(*config.LoadOptions) error
	if o.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(o.Region))
	}
	if o.Profile != "" {
		loadOpts = append(loadOpts, config.WithSharedConfigProfile(o.Profile))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	var clientOpts []func(*dynamodb.Options)
	if o.Endpoint != "" {
		endpoint := o.Endpoint
		clientOpts = append(clientOpts, func(opts *dynamodb.Options) { opts.BaseEndpoint = aws.String(endpoint) })
	}
	return dynamodb.NewFromConfig(cfg, clientOpts...), nil
}

func parseConnectOptions(opts map[string]any) (connectOptions, error) {
	var o connectOptions
	for key, raw := range opts {
		var dst *string
		switch key {
		case "region":
			dst = &o.Region
		case "endpoint":
			dst = &o.Endpoint
		case "profile":
			dst = &o.Profile
		default:
			return o, fmt.Errorf("unknown option %q (known: region, endpoint, profile)", key)
		}
		value, ok := raw.(string)
		if !ok {
			return o, fmt.Errorf("option %q: expected string, got %s", key, noxyTypeName(raw))
		}
		*dst = value
	}
	return o, nil
}

// noxyTypeName names a decoded argument in Noxy's vocabulary, for messages.
func noxyTypeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case int64:
		return "int"
	case float64:
		return "float"
	case string:
		return "string"
	case []byte:
		return "bytes"
	case []any:
		return "array"
	case map[string]any, map[int64]any, map[any]any:
		return "map"
	case noxyplugin.Struct:
		return "struct"
	}
	return fmt.Sprintf("%T", v)
}

// connect: dynamodb_connect(options) -> int. A failed dial mints nothing.
func (s *service) connect(ctx context.Context, opts map[string]any) (int64, error) {
	o, err := parseConnectOptions(opts)
	if err != nil {
		return 0, err
	}
	client, err := s.dial(ctx, o)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	s.clients[s.next] = client
	return s.next, nil
}

// closeClient: dynamodb_close(handle) -> void. Closing twice is an error,
// like any other use of a dead handle.
func (s *service) closeClient(_ context.Context, handle int64) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.clients[handle]; !ok {
		return nil, unknownHandle(handle)
	}
	delete(s.clients, handle)
	return nil, nil
}

func (s *service) client(handle int64) (dynamoAPI, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	client, ok := s.clients[handle]
	if !ok {
		return nil, unknownHandle(handle)
	}
	return client, nil
}

func unknownHandle(handle int64) error {
	return fmt.Errorf("unknown client handle %d (closed, or never returned by connect)", handle)
}

// putItem: dynamodb_put_item(handle, table, item) -> void.
func (s *service) putItem(ctx context.Context, handle int64, table string, item map[string]any) (any, error) {
	client, err := s.client(handle)
	if err != nil {
		return nil, err
	}
	av, err := marshalMap(item)
	if err != nil {
		return nil, fmt.Errorf("item: %w", err)
	}
	if _, err := client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(table), Item: av}); err != nil {
		return nil, err
	}
	return nil, nil
}

// getItem: dynamodb_get_item(handle, table, key) -> any: the item as a map,
// or null when the key does not exist (the reason the export is `any`, not
// `map[string]any` — the host rejects null for a declared map).
func (s *service) getItem(ctx context.Context, handle int64, table string, key map[string]any) (any, error) {
	client, err := s.client(handle)
	if err != nil {
		return nil, err
	}
	avKey, err := marshalMap(key)
	if err != nil {
		return nil, fmt.Errorf("key: %w", err)
	}
	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(table), Key: avKey})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, nil
	}
	item, err := unmarshalItem(out.Item)
	if err != nil {
		return nil, err
	}
	return item, nil
}

// updateItem: dynamodb_update_item(handle, table, key, update_expression,
// expression_values) -> void. Empty expression values are omitted (DynamoDB
// rejects an empty ExpressionAttributeValues map).
func (s *service) updateItem(ctx context.Context, handle int64, table string, key map[string]any, expression string, values map[string]any) (any, error) {
	client, err := s.client(handle)
	if err != nil {
		return nil, err
	}
	avKey, err := marshalMap(key)
	if err != nil {
		return nil, fmt.Errorf("key: %w", err)
	}
	in := &dynamodb.UpdateItemInput{TableName: aws.String(table), Key: avKey, UpdateExpression: aws.String(expression)}
	if len(values) > 0 {
		if in.ExpressionAttributeValues, err = marshalMap(values); err != nil {
			return nil, fmt.Errorf("expression values: %w", err)
		}
	}
	if _, err := client.UpdateItem(ctx, in); err != nil {
		return nil, err
	}
	return nil, nil
}

// deleteItem: dynamodb_delete_item(handle, table, key) -> void.
func (s *service) deleteItem(ctx context.Context, handle int64, table string, key map[string]any) (any, error) {
	client, err := s.client(handle)
	if err != nil {
		return nil, err
	}
	avKey, err := marshalMap(key)
	if err != nil {
		return nil, fmt.Errorf("key: %w", err)
	}
	if _, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(table), Key: avKey}); err != nil {
		return nil, err
	}
	return nil, nil
}

// scan: dynamodb_scan(handle, table, limit) -> map[string]any[]: up to
// `limit` items, following pagination only as far as needed; limit 0 means
// every item of the table (opt-in — on a large table that is bounded only
// by the host's call deadline).
func (s *service) scan(ctx context.Context, handle int64, table string, limit int64) ([]map[string]any, error) {
	client, err := s.client(handle)
	if err != nil {
		return nil, err
	}
	return collect(ctx, limit, scanFetcher(client, table))
}

// scanPage: dynamodb_scan_page(handle, table, limit, start_key) ->
// map[string]any: ONE request — {"items": [...], "last_key": key | null}.
// Pass last_key back as start_key for the next page; null means the end.
func (s *service) scanPage(ctx context.Context, handle int64, table string, limit int64, startKey map[string]any) (map[string]any, error) {
	client, err := s.client(handle)
	if err != nil {
		return nil, err
	}
	return page(ctx, limit, startKey, scanFetcher(client, table))
}

// query: dynamodb_query(handle, table, key_condition, expression_values,
// limit) -> map[string]any[], with the same limit semantics as scan.
func (s *service) query(ctx context.Context, handle int64, table string, keyCondition string, values map[string]any, limit int64) ([]map[string]any, error) {
	client, err := s.client(handle)
	if err != nil {
		return nil, err
	}
	fetch, err := queryFetcher(client, table, keyCondition, values)
	if err != nil {
		return nil, err
	}
	return collect(ctx, limit, fetch)
}

// queryPage: dynamodb_query_page(handle, table, key_condition,
// expression_values, limit, start_key) -> map[string]any, one request like
// scanPage.
func (s *service) queryPage(ctx context.Context, handle int64, table string, keyCondition string, values map[string]any, limit int64, startKey map[string]any) (map[string]any, error) {
	client, err := s.client(handle)
	if err != nil {
		return nil, err
	}
	fetch, err := queryFetcher(client, table, keyCondition, values)
	if err != nil {
		return nil, err
	}
	return page(ctx, limit, startKey, fetch)
}

// queryPageHandler is the untyped adapter for the six-argument export.
func (s *service) queryPageHandler(ctx context.Context, args noxyplugin.Args) (any, error) {
	if len(args) != 6 {
		return nil, fmt.Errorf("expected 6 arguments, got %d", len(args))
	}
	handle, err := args.Int(0)
	if err != nil {
		return nil, err
	}
	table, err := args.String(1)
	if err != nil {
		return nil, err
	}
	keyCondition, err := args.String(2)
	if err != nil {
		return nil, err
	}
	values, err := optionalMap(args, 3)
	if err != nil {
		return nil, err
	}
	limit, err := args.Int(4)
	if err != nil {
		return nil, err
	}
	startKey, err := optionalMap(args, 5)
	if err != nil {
		return nil, err
	}
	return s.queryPage(ctx, handle, table, keyCondition, values, limit, startKey)
}

// optionalMap reads a map argument that may be null (a nil map).
func optionalMap(args noxyplugin.Args, i int) (map[string]any, error) {
	switch v := args[i].(type) {
	case nil:
		return nil, nil
	case map[string]any:
		return v, nil
	case map[any]any:
		if len(v) == 0 {
			return map[string]any{}, nil
		}
	}
	return nil, fmt.Errorf("argument %d: expected map with string keys, got %s", i+1, noxyTypeName(args[i]))
}

// pageFetcher issues one Scan or Query request: at most `limit` items (nil =
// DynamoDB's natural page), from `start` (nil = the beginning).
type pageFetcher func(ctx context.Context, limit *int32, start map[string]types.AttributeValue) (items []map[string]types.AttributeValue, lastKey map[string]types.AttributeValue, err error)

func scanFetcher(client dynamoAPI, table string) pageFetcher {
	return func(ctx context.Context, limit *int32, start map[string]types.AttributeValue) ([]map[string]types.AttributeValue, map[string]types.AttributeValue, error) {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(table), Limit: limit, ExclusiveStartKey: start})
		if err != nil {
			return nil, nil, err
		}
		return out.Items, out.LastEvaluatedKey, nil
	}
}

func queryFetcher(client dynamoAPI, table string, keyCondition string, values map[string]any) (pageFetcher, error) {
	var avValues map[string]types.AttributeValue
	if len(values) > 0 {
		var err error
		if avValues, err = marshalMap(values); err != nil {
			return nil, fmt.Errorf("expression values: %w", err)
		}
	}
	return func(ctx context.Context, limit *int32, start map[string]types.AttributeValue) ([]map[string]types.AttributeValue, map[string]types.AttributeValue, error) {
		out, err := client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(table),
			KeyConditionExpression:    aws.String(keyCondition),
			ExpressionAttributeValues: avValues,
			Limit:                     limit,
			ExclusiveStartKey:         start,
		})
		if err != nil {
			return nil, nil, err
		}
		return out.Items, out.LastEvaluatedKey, nil
	}, nil
}

// collect follows pagination until the table (or partition) is exhausted or
// `limit` items are in hand; every request asks for no more than the
// remainder, so a capped call never reads past what it returns. limit 0 =
// no cap; the host's call deadline (ctx) is then the only bound.
func collect(ctx context.Context, limit int64, fetch pageFetcher) ([]map[string]any, error) {
	if limit < 0 {
		return nil, fmt.Errorf("limit must not be negative, got %d", limit)
	}
	items := []map[string]any{}
	var start map[string]types.AttributeValue
	for {
		var want *int32
		if limit > 0 {
			want = requestLimit(limit - int64(len(items)))
		}
		raw, lastKey, err := fetch(ctx, want, start)
		if err != nil {
			return nil, err
		}
		decoded, err := unmarshalItems(raw)
		if err != nil {
			return nil, err
		}
		items = append(items, decoded...)
		if limit > 0 && int64(len(items)) >= limit {
			return items[:limit], nil
		}
		if len(lastKey) == 0 {
			return items, nil
		}
		start = lastKey
	}
}

// page issues exactly one request and returns {"items", "last_key"}; a
// null last_key marks the end (DynamoDB may still return a key on the final
// item when it stopped because of Limit — the next page is then empty).
func page(ctx context.Context, limit int64, startKey map[string]any, fetch pageFetcher) (map[string]any, error) {
	if limit < 0 {
		return nil, fmt.Errorf("limit must not be negative, got %d", limit)
	}
	var want *int32
	if limit > 0 {
		want = requestLimit(limit)
	}
	var start map[string]types.AttributeValue
	if len(startKey) > 0 {
		var err error
		if start, err = marshalMap(startKey); err != nil {
			return nil, fmt.Errorf("start key: %w", err)
		}
	}
	raw, lastKey, err := fetch(ctx, want, start)
	if err != nil {
		return nil, err
	}
	items, err := unmarshalItems(raw)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"items": items, "last_key": nil}
	if len(lastKey) > 0 {
		key, err := unmarshalItem(lastKey)
		if err != nil {
			return nil, fmt.Errorf("last key: %w", err)
		}
		out["last_key"] = key
	}
	return out, nil
}

func requestLimit(n int64) *int32 {
	if n > math.MaxInt32 {
		n = math.MaxInt32
	}
	return aws.Int32(int32(n))
}

// marshalMap turns a decoded Noxy map into DynamoDB attribute values. The
// NXB types map directly (int → N, float → N, bool → BOOL, string → S,
// bytes → B, array → L, map → M, null → NULL); a Noxy struct crosses as
// noxyplugin.Struct and is flattened to a map of its fields first.
func marshalMap(m map[string]any) (map[string]types.AttributeValue, error) {
	return attributevalue.MarshalMap(normalize(m))
}

func normalize(v any) any {
	switch x := v.(type) {
	case noxyplugin.Struct:
		out := make(map[string]any, len(x.Fields))
		for _, f := range x.Fields {
			out[f.Name] = normalize(f.Value)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, item := range x {
			out[k] = normalize(item)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = normalize(item)
		}
		return out
	}
	return v
}

// unmarshalItem decodes an item into plain Go values; numbers come back as
// float64 (Noxy float), exactly as they did through the JSON plugin.
func unmarshalItem(raw map[string]types.AttributeValue) (map[string]any, error) {
	var item map[string]any
	if err := attributevalue.UnmarshalMap(raw, &item); err != nil {
		return nil, fmt.Errorf("decode item: %w", err)
	}
	return item, nil
}

func unmarshalItems(raw []map[string]types.AttributeValue) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(raw))
	for _, r := range raw {
		item, err := unmarshalItem(r)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, nil
}
