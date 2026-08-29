package main

import (
	"context"
	"errors"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/estevaofon/noxy/sdk/noxyplugin"
)

// fakeDynamo records the last input of every operation and replies with
// canned outputs, so the handlers are exercised without AWS.
type fakeDynamo struct {
	err      error
	putIn    *dynamodb.PutItemInput
	getIn    *dynamodb.GetItemInput
	getOut   *dynamodb.GetItemOutput
	updateIn *dynamodb.UpdateItemInput
	deleteIn *dynamodb.DeleteItemInput
	scanIn   []*dynamodb.ScanInput
	scanOut  []*dynamodb.ScanOutput
	queryIn  []*dynamodb.QueryInput
	queryOut []*dynamodb.QueryOutput
}

func (f *fakeDynamo) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.putIn = in
	return &dynamodb.PutItemOutput{}, f.err
}

func (f *fakeDynamo) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	f.getIn = in
	if f.getOut == nil {
		return &dynamodb.GetItemOutput{}, f.err
	}
	return f.getOut, f.err
}

func (f *fakeDynamo) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.updateIn = in
	return &dynamodb.UpdateItemOutput{}, f.err
}

func (f *fakeDynamo) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	f.deleteIn = in
	return &dynamodb.DeleteItemOutput{}, f.err
}

func (f *fakeDynamo) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	f.scanIn = append(f.scanIn, in)
	if f.err != nil {
		return nil, f.err
	}
	if len(f.scanOut) == 0 {
		return &dynamodb.ScanOutput{}, nil
	}
	out := f.scanOut[0]
	f.scanOut = f.scanOut[1:]
	return out, nil
}

func (f *fakeDynamo) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	f.queryIn = append(f.queryIn, in)
	if f.err != nil {
		return nil, f.err
	}
	if len(f.queryOut) == 0 {
		return &dynamodb.QueryOutput{}, nil
	}
	out := f.queryOut[0]
	f.queryOut = f.queryOut[1:]
	return out, nil
}

func serviceFor(api dynamoAPI) *service {
	return newService(func(context.Context, connectOptions) (dynamoAPI, error) { return api, nil })
}

func mustConnect(t *testing.T, s *service) int64 {
	t.Helper()
	h, err := s.connect(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return h
}

func TestConnectMintsSequentialHandles(t *testing.T) {
	s := serviceFor(&fakeDynamo{})
	if h := mustConnect(t, s); h != 1 {
		t.Fatalf("first handle = %d, want 1", h)
	}
	if h := mustConnect(t, s); h != 2 {
		t.Fatalf("second handle = %d, want 2", h)
	}
}

func TestConnectPassesParsedOptionsToDialer(t *testing.T) {
	var got connectOptions
	s := newService(func(_ context.Context, o connectOptions) (dynamoAPI, error) {
		got = o
		return &fakeDynamo{}, nil
	})
	_, err := s.connect(context.Background(), map[string]any{
		"region":   "sa-east-1",
		"endpoint": "http://localhost:8000",
		"profile":  "dev",
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	want := connectOptions{Region: "sa-east-1", Endpoint: "http://localhost:8000", Profile: "dev"}
	if got != want {
		t.Fatalf("dialer got %+v, want %+v", got, want)
	}
}

func TestConnectRejectsBadOptions(t *testing.T) {
	s := serviceFor(&fakeDynamo{})
	cases := []struct {
		opts map[string]any
		want string
	}{
		{map[string]any{"region": int64(5)}, `option "region": expected string, got int`},
		{map[string]any{"bogus": "x"}, `unknown option "bogus"`},
	}
	for _, c := range cases {
		_, err := s.connect(context.Background(), c.opts)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("connect(%v) error = %v, want it to contain %q", c.opts, err, c.want)
		}
	}
}

func TestConnectFailureMintsNoHandle(t *testing.T) {
	calls := 0
	s := newService(func(context.Context, connectOptions) (dynamoAPI, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("no credentials")
		}
		return &fakeDynamo{}, nil
	})
	if _, err := s.connect(context.Background(), map[string]any{}); err == nil || !strings.Contains(err.Error(), "no credentials") {
		t.Fatalf("first connect error = %v, want the dialer's error", err)
	}
	if h := mustConnect(t, s); h != 1 {
		t.Fatalf("handle after a failed connect = %d, want 1 (nothing was minted)", h)
	}
}

func TestCloseReleasesHandle(t *testing.T) {
	s := serviceFor(&fakeDynamo{})
	h := mustConnect(t, s)
	if res, err := s.closeClient(context.Background(), h); err != nil || res != nil {
		t.Fatalf("close = (%v, %v), want (nil, nil)", res, err)
	}
	if _, err := s.putItem(context.Background(), h, "t", map[string]any{"id": "x"}); err == nil || !strings.Contains(err.Error(), "unknown client handle 1") {
		t.Fatalf("put after close error = %v, want unknown client handle 1", err)
	}
	if _, err := s.closeClient(context.Background(), h); err == nil || !strings.Contains(err.Error(), "unknown client handle 1") {
		t.Fatalf("second close error = %v, want unknown client handle 1", err)
	}
}

func TestUnknownHandleIsAnError(t *testing.T) {
	s := serviceFor(&fakeDynamo{})
	_, err := s.scan(context.Background(), 7, "t")
	if err == nil || !strings.Contains(err.Error(), "unknown client handle 7") {
		t.Fatalf("scan error = %v, want unknown client handle 7", err)
	}
}

func TestPutItemMarshalsNoxyValues(t *testing.T) {
	api := &fakeDynamo{}
	s := serviceFor(api)
	h := mustConnect(t, s)
	res, err := s.putItem(context.Background(), h, "Users", map[string]any{
		"id":      "u1",
		"n":       int64(3),
		"f":       2.5,
		"ok":      true,
		"tags":    []any{"a", "b"},
		"meta":    map[string]any{"k": int64(1)},
		"nothing": nil,
		"raw":     []byte{1, 2},
	})
	if err != nil {
		t.Fatalf("putItem: %v", err)
	}
	if res != nil {
		t.Fatalf("putItem result = %v, want nil (void)", res)
	}
	if aws.ToString(api.putIn.TableName) != "Users" {
		t.Fatalf("TableName = %q, want Users", aws.ToString(api.putIn.TableName))
	}
	item := api.putIn.Item
	if v, ok := item["id"].(*types.AttributeValueMemberS); !ok || v.Value != "u1" {
		t.Fatalf("id = %#v, want S u1", item["id"])
	}
	if v, ok := item["n"].(*types.AttributeValueMemberN); !ok || v.Value != "3" {
		t.Fatalf("n = %#v, want N 3", item["n"])
	}
	if v, ok := item["f"].(*types.AttributeValueMemberN); !ok || v.Value != "2.5" {
		t.Fatalf("f = %#v, want N 2.5", item["f"])
	}
	if v, ok := item["ok"].(*types.AttributeValueMemberBOOL); !ok || !v.Value {
		t.Fatalf("ok = %#v, want BOOL true", item["ok"])
	}
	if v, ok := item["tags"].(*types.AttributeValueMemberL); !ok || len(v.Value) != 2 {
		t.Fatalf("tags = %#v, want L of 2", item["tags"])
	}
	m, ok := item["meta"].(*types.AttributeValueMemberM)
	if !ok {
		t.Fatalf("meta = %#v, want M", item["meta"])
	}
	if v, ok := m.Value["k"].(*types.AttributeValueMemberN); !ok || v.Value != "1" {
		t.Fatalf("meta.k = %#v, want N 1", m.Value["k"])
	}
	if v, ok := item["nothing"].(*types.AttributeValueMemberNULL); !ok || !v.Value {
		t.Fatalf("nothing = %#v, want NULL", item["nothing"])
	}
	if v, ok := item["raw"].(*types.AttributeValueMemberB); !ok || len(v.Value) != 2 {
		t.Fatalf("raw = %#v, want B of 2 bytes", item["raw"])
	}
}

func TestPutItemFlattensStructsIntoMaps(t *testing.T) {
	api := &fakeDynamo{}
	s := serviceFor(api)
	h := mustConnect(t, s)
	point := noxyplugin.Struct{Name: "Point", Fields: []noxyplugin.Field{{Name: "x", Value: int64(1)}, {Name: "y", Value: int64(2)}}}
	if _, err := s.putItem(context.Background(), h, "T", map[string]any{"id": "p", "p": point, "ps": []any{point}}); err != nil {
		t.Fatalf("putItem: %v", err)
	}
	m, ok := api.putIn.Item["p"].(*types.AttributeValueMemberM)
	if !ok {
		t.Fatalf("p = %#v, want M", api.putIn.Item["p"])
	}
	if v, ok := m.Value["y"].(*types.AttributeValueMemberN); !ok || v.Value != "2" {
		t.Fatalf("p.y = %#v, want N 2", m.Value["y"])
	}
	l, ok := api.putIn.Item["ps"].(*types.AttributeValueMemberL)
	if !ok || len(l.Value) != 1 {
		t.Fatalf("ps = %#v, want L of 1", api.putIn.Item["ps"])
	}
	if _, ok := l.Value[0].(*types.AttributeValueMemberM); !ok {
		t.Fatalf("ps[0] = %#v, want M", l.Value[0])
	}
}

func TestGetItemMissingIsNull(t *testing.T) {
	api := &fakeDynamo{}
	s := serviceFor(api)
	h := mustConnect(t, s)
	res, err := s.getItem(context.Background(), h, "Users", map[string]any{"id": "nope"})
	if err != nil {
		t.Fatalf("getItem: %v", err)
	}
	if res != nil {
		t.Fatalf("getItem result = %#v, want nil", res)
	}
	if v, ok := api.getIn.Key["id"].(*types.AttributeValueMemberS); !ok || v.Value != "nope" {
		t.Fatalf("key = %#v, want S nope", api.getIn.Key["id"])
	}
}

func TestGetItemNumbersComeBackAsFloat(t *testing.T) {
	api := &fakeDynamo{getOut: &dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
		"id": &types.AttributeValueMemberS{Value: "u1"},
		"n":  &types.AttributeValueMemberN{Value: "3"},
	}}}
	s := serviceFor(api)
	h := mustConnect(t, s)
	res, err := s.getItem(context.Background(), h, "Users", map[string]any{"id": "u1"})
	if err != nil {
		t.Fatalf("getItem: %v", err)
	}
	item, ok := res.(map[string]any)
	if !ok {
		t.Fatalf("getItem result = %#v, want map[string]any", res)
	}
	if item["id"] != "u1" || item["n"] != float64(3) {
		t.Fatalf("item = %#v, want id u1 and n 3.0", item)
	}
}

func TestUpdateItemPassesExpression(t *testing.T) {
	api := &fakeDynamo{}
	s := serviceFor(api)
	h := mustConnect(t, s)
	res, err := s.updateItem(context.Background(), h, "Users", map[string]any{"id": "u1"}, "SET age = :a", map[string]any{":a": int64(30)})
	if err != nil || res != nil {
		t.Fatalf("updateItem = (%v, %v), want (nil, nil)", res, err)
	}
	in := api.updateIn
	if aws.ToString(in.TableName) != "Users" || aws.ToString(in.UpdateExpression) != "SET age = :a" {
		t.Fatalf("update input = %+v", in)
	}
	if v, ok := in.Key["id"].(*types.AttributeValueMemberS); !ok || v.Value != "u1" {
		t.Fatalf("key = %#v, want S u1", in.Key["id"])
	}
	if v, ok := in.ExpressionAttributeValues[":a"].(*types.AttributeValueMemberN); !ok || v.Value != "30" {
		t.Fatalf(":a = %#v, want N 30", in.ExpressionAttributeValues[":a"])
	}
}

func TestUpdateItemOmitsEmptyExpressionValues(t *testing.T) {
	api := &fakeDynamo{}
	s := serviceFor(api)
	h := mustConnect(t, s)
	if _, err := s.updateItem(context.Background(), h, "Users", map[string]any{"id": "u1"}, "REMOVE age", map[string]any{}); err != nil {
		t.Fatalf("updateItem: %v", err)
	}
	if api.updateIn.ExpressionAttributeValues != nil {
		t.Fatalf("ExpressionAttributeValues = %#v, want nil when no values are given", api.updateIn.ExpressionAttributeValues)
	}
}

func TestDeleteItemPassesKey(t *testing.T) {
	api := &fakeDynamo{}
	s := serviceFor(api)
	h := mustConnect(t, s)
	res, err := s.deleteItem(context.Background(), h, "Users", map[string]any{"id": "u1"})
	if err != nil || res != nil {
		t.Fatalf("deleteItem = (%v, %v), want (nil, nil)", res, err)
	}
	if aws.ToString(api.deleteIn.TableName) != "Users" {
		t.Fatalf("TableName = %q", aws.ToString(api.deleteIn.TableName))
	}
	if v, ok := api.deleteIn.Key["id"].(*types.AttributeValueMemberS); !ok || v.Value != "u1" {
		t.Fatalf("key = %#v, want S u1", api.deleteIn.Key["id"])
	}
}

func TestScanFollowsPagination(t *testing.T) {
	api := &fakeDynamo{scanOut: []*dynamodb.ScanOutput{
		{
			Items: []map[string]types.AttributeValue{
				{"id": &types.AttributeValueMemberS{Value: "a"}},
				{"id": &types.AttributeValueMemberS{Value: "b"}},
			},
			LastEvaluatedKey: map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "b"}},
		},
		{Items: []map[string]types.AttributeValue{{"id": &types.AttributeValueMemberS{Value: "c"}}}},
	}}
	s := serviceFor(api)
	h := mustConnect(t, s)
	items, err := s.scan(context.Background(), h, "Users")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(items) != 3 || items[2]["id"] != "c" {
		t.Fatalf("items = %#v, want a, b, c", items)
	}
	if len(api.scanIn) != 2 || aws.ToString(api.scanIn[0].TableName) != "Users" {
		t.Fatalf("scan calls = %d (table %q), want 2 calls on Users", len(api.scanIn), aws.ToString(api.scanIn[0].TableName))
	}
	if api.scanIn[1].ExclusiveStartKey == nil {
		t.Fatal("second page did not carry ExclusiveStartKey")
	}
}

func TestScanOfEmptyTableIsAnEmptyArray(t *testing.T) {
	s := serviceFor(&fakeDynamo{})
	h := mustConnect(t, s)
	items, err := s.scan(context.Background(), h, "Users")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if items == nil || len(items) != 0 {
		t.Fatalf("items = %#v, want a non-nil empty slice", items)
	}
}

func TestQueryPassesConditionAndFollowsPagination(t *testing.T) {
	api := &fakeDynamo{queryOut: []*dynamodb.QueryOutput{
		{
			Items:            []map[string]types.AttributeValue{{"id": &types.AttributeValueMemberS{Value: "a"}}},
			LastEvaluatedKey: map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "a"}},
		},
		{Items: []map[string]types.AttributeValue{{"id": &types.AttributeValueMemberS{Value: "a"}, "n": &types.AttributeValueMemberN{Value: "2"}}}},
	}}
	s := serviceFor(api)
	h := mustConnect(t, s)
	items, err := s.query(context.Background(), h, "Users", "id = :id", map[string]any{":id": "a"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(items) != 2 || items[1]["n"] != float64(2) {
		t.Fatalf("items = %#v, want two items with n = 2.0 on the second", items)
	}
	in := api.queryIn[0]
	if aws.ToString(in.TableName) != "Users" || aws.ToString(in.KeyConditionExpression) != "id = :id" {
		t.Fatalf("query input = %+v", in)
	}
	if v, ok := in.ExpressionAttributeValues[":id"].(*types.AttributeValueMemberS); !ok || v.Value != "a" {
		t.Fatalf(":id = %#v, want S a", in.ExpressionAttributeValues[":id"])
	}
	if len(api.queryIn) != 2 || api.queryIn[1].ExclusiveStartKey == nil {
		t.Fatalf("query calls = %d, want 2 with ExclusiveStartKey on the second", len(api.queryIn))
	}
}

func TestAWSErrorsSurfaceAsHandlerErrors(t *testing.T) {
	api := &fakeDynamo{err: errors.New("ResourceNotFoundException: Requested resource not found")}
	s := serviceFor(api)
	h := mustConnect(t, s)
	if _, err := s.putItem(context.Background(), h, "Missing", map[string]any{"id": "x"}); err == nil || !strings.Contains(err.Error(), "ResourceNotFoundException") {
		t.Fatalf("putItem error = %v, want the AWS error", err)
	}
	if _, err := s.scan(context.Background(), h, "Missing"); err == nil || !strings.Contains(err.Error(), "ResourceNotFoundException") {
		t.Fatalf("scan error = %v, want the AWS error", err)
	}
}

// The manifest is the wire truth: every [[export]] needs a handler and every
// handler needs an export, or the handshake (or a user) is surprised.
func TestManifestExportsMatchHandlers(t *testing.T) {
	data, err := os.ReadFile("noxy_ext.toml")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var declared []string
	for _, m := range regexp.MustCompile(`(?m)^name\s*=\s*"(dynamodb_[a-z_]+)"`).FindAllStringSubmatch(string(data), -1) {
		declared = append(declared, m[1])
	}
	var registered []string
	for name := range serviceFor(&fakeDynamo{}).handlers() {
		registered = append(registered, name)
	}
	sort.Strings(declared)
	sort.Strings(registered)
	if strings.Join(declared, ",") != strings.Join(registered, ",") {
		t.Fatalf("manifest exports %v != handlers %v", declared, registered)
	}
}
