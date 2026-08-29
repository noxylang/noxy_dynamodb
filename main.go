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
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/estevaofon/noxy/sdk/noxyplugin"
)

func main() {
	p := noxyplugin.New()
	for name, h := range newService(connectAWS).handlers() {
		p.Handle(name, h)
	}
	p.Main()
}

// dynamoAPI is the slice of *dynamodb.Client the handlers use; tests plug in
// a fake. It also satisfies the SDK's Scan/Query paginator interfaces.
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

// handlers maps every [[export]] of noxy_ext.toml to its typed handler.
func (s *service) handlers() map[string]noxyplugin.Handler {
	return map[string]noxyplugin.Handler{
		"dynamodb_connect":     noxyplugin.Func1(s.connect),
		"dynamodb_put_item":    noxyplugin.Func3(s.putItem),
		"dynamodb_get_item":    noxyplugin.Func3(s.getItem),
		"dynamodb_update_item": noxyplugin.Func5(s.updateItem),
		"dynamodb_delete_item": noxyplugin.Func3(s.deleteItem),
		"dynamodb_scan":        noxyplugin.Func2(s.scan),
		"dynamodb_query":       noxyplugin.Func4(s.query),
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

// scan: dynamodb_scan(handle, table) -> map[string]any[]: every item of the
// table, following pagination; cancelled by the host's call deadline.
func (s *service) scan(ctx context.Context, handle int64, table string) ([]map[string]any, error) {
	client, err := s.client(handle)
	if err != nil {
		return nil, err
	}
	items := []map[string]any{}
	pager := dynamodb.NewScanPaginator(client, &dynamodb.ScanInput{TableName: aws.String(table)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		decoded, err := unmarshalItems(page.Items)
		if err != nil {
			return nil, err
		}
		items = append(items, decoded...)
	}
	return items, nil
}

// query: dynamodb_query(handle, table, key_condition, expression_values) ->
// map[string]any[], following pagination like scan.
func (s *service) query(ctx context.Context, handle int64, table string, keyCondition string, values map[string]any) ([]map[string]any, error) {
	client, err := s.client(handle)
	if err != nil {
		return nil, err
	}
	in := &dynamodb.QueryInput{TableName: aws.String(table), KeyConditionExpression: aws.String(keyCondition)}
	if len(values) > 0 {
		if in.ExpressionAttributeValues, err = marshalMap(values); err != nil {
			return nil, fmt.Errorf("expression values: %w", err)
		}
	}
	items := []map[string]any{}
	pager := dynamodb.NewQueryPaginator(client, in)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		decoded, err := unmarshalItems(page.Items)
		if err != nil {
			return nil, err
		}
		items = append(items, decoded...)
	}
	return items, nil
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
