package bq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/googleapis/gax-go/v2"
	"github.com/mertkayhan/piperr/pkg/arrowproto"
	"github.com/mertkayhan/piperr/pkg/catalog"
	"github.com/mertkayhan/piperr/pkg/tmpl"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var ErrStreamNameMissing = errors.New("stream name not present in metadata")

const (
	RegularMode int32 = 42
	RetryMode   int32 = 69
)

type DirectClient struct {
	location      string
	projectID     string
	datasetID     string
	pipeName      string
	streamCatalog *catalog.Catalog
}

type streamState struct {
	curOffset     int64
	schema        *arrow.Schema
	msgDesc       protoreflect.MessageDescriptor
	managedStream *managedwriter.ManagedStream
	tableRef      *bigquery.Table
}

type writeContext struct {
	result *managedwriter.AppendResult
	state  streamState
	batch  arrow.Record
}

type writeBarrier struct {
	mu *atomic.Int32
}

func (w *writeBarrier) wait() {
	for w.mu.Load() != RegularMode {
		time.Sleep(10 * time.Millisecond)
	}
}

func (w *writeBarrier) lock() {
	for !w.mu.CompareAndSwap(RegularMode, RetryMode) {
		time.Sleep(10 * time.Millisecond)
	}
}

func (w *writeBarrier) unlock() {
	for !w.mu.CompareAndSwap(RetryMode, RegularMode) {
		time.Sleep(10 * time.Millisecond)
	}
}

func NewDirectClient(opts *ClientOpts) (*DirectClient, error) {
	return &DirectClient{
		location:      opts.Location,
		projectID:     opts.ProjectID,
		datasetID:     opts.DatasetID,
		pipeName:      opts.PipeName,
		streamCatalog: opts.StreamCatalog,
	}, nil
}

func initStream(ctx context.Context, streamName string, bqClient *bigquery.Client, mwClient *managedwriter.Client, rec arrow.Record, c *DirectClient) (*streamState, error) {
	tmpTable := fmt.Sprintf("%s_Tmp_%d", streamName, time.Now().UnixNano())
	datasetRef := bqClient.Dataset(c.datasetID)
	schema := rec.Schema()
	if !tableExists(ctx, datasetRef, streamName) {
		log.Println("target table missing, will create it...")
		if err := createTable(ctx, schema, c.location, datasetRef.Table(streamName), time.Time{}); err != nil {
			return nil, fmt.Errorf("initStream: %w", err)
		}
	}
	if err := createTable(ctx, schema, c.location, datasetRef.Table(tmpTable), time.Now().AddDate(0, 0, 2)); err != nil {
		return nil, fmt.Errorf("initStream: %w", err)
	}
	pendingStream, err := createWriteStream(ctx, mwClient, c.projectID, c.datasetID, tmpTable, c.location)
	if err != nil {
		return nil, fmt.Errorf("initStream: %w", err)
	}
	managedStream, msgDesc, err := createManagedStream(ctx, schema, streamName, mwClient, pendingStream.Name)
	if err != nil {
		return nil, fmt.Errorf("initStream: %w", err)
	}
	return &streamState{
		curOffset:     int64(0),
		schema:        schema,
		msgDesc:       msgDesc,
		managedStream: managedStream,
		tableRef:      datasetRef.Table(tmpTable),
	}, nil
}

func run(ctx context.Context, wg *sync.WaitGroup, streamName string, recCh <-chan arrow.Record, bqClient *bigquery.Client, mwClient *managedwriter.Client, c *DirectClient, errCh chan<- error) {
	results := make(chan *writeContext, 100)
	childWg := &sync.WaitGroup{}
	mu := &atomic.Int32{}
	mu.Store(RegularMode)
	barrier := &writeBarrier{mu: mu}
	defer wg.Done()
	var (
		state *streamState
		err   error
	)
	childWg.Add(1)
	go checkWrite(ctx, childWg, results, errCh, barrier)
	for rec := range recCh {
		select {
		case <-ctx.Done():
			return
		default:
			if state == nil {
				state, err = initStream(ctx, streamName, bqClient, mwClient, rec, c)
				if err != nil {
					errCh <- fmt.Errorf("run: %w", err)
					return
				}
				defer deleteTable(ctx, state.tableRef)
			}
			barrier.wait()
			if err := write(ctx, state, rec, results); err != nil {
				errCh <- fmt.Errorf("run: %w", err)
				return
			}
			state.curOffset += rec.NumRows()
		}
	}
	// there wont be any more results
	close(results)
	childWg.Wait()
	if err := finalizeManagedStream(ctx, state.managedStream); err != nil {
		errCh <- fmt.Errorf("run: %w", err)
		return
	}
	if err := commitWriteStream(ctx, state.managedStream, mwClient); err != nil {
		errCh <- fmt.Errorf("run: %w", err)
		return
	}
	mode, err := c.streamCatalog.GetMode(streamName)
	if err != nil {
		errCh <- fmt.Errorf("run: %w", err)
		return
	}
	query, err := getFinalizerQuery(*mode, &tmpl.Query{
		OriginalTable: fmt.Sprintf("%s.%s.%s", c.projectID, c.datasetID, streamName),
		TmpTable:      fmt.Sprintf("%s.%s.%s", c.projectID, c.datasetID, state.tableRef.TableID),
	})
	if err != nil {
		errCh <- fmt.Errorf("run: %w", err)
		return
	}
	log.Println("running finalizer...")
	q := bqClient.Query(query)
	job, err := q.Run(ctx)
	if err != nil {
		errCh <- fmt.Errorf("run: %w", err)
		return
	}
	status, err := job.Wait(ctx)
	if err != nil {
		errCh <- fmt.Errorf("run: %w", err)
		return
	}
	if status.Err() != nil {
		errCh <- fmt.Errorf("run: %w", status.Err())
		return
	}
}

func getFinalizerQuery(mode catalog.SyncMode, q *tmpl.Query) (string, error) {
	switch mode {
	case catalog.SyncModeFullRefreshOverwrite:
		return tmpl.CreateFullRefreshOverwriteFinalizer(q)
	default:
		return "", fmt.Errorf("unknown sync mode")
	}
}

func routeRecords(ctx context.Context, wg *sync.WaitGroup, reader *ipc.Reader, streamChMap map[string]chan arrow.Record, errCh chan<- error) {
	defer func() {
		for _, ch := range streamChMap {
			close(ch)
		}
		wg.Done()
	}()
	for reader.Next() {
		select {
		case <-ctx.Done():
			return
		default:
			rec := reader.Record()
			metadata := reader.Schema().Metadata()
			targetStream, ok := metadata.GetValue("StreamName")
			if !ok {
				errCh <- ErrStreamNameMissing
				return
			}
			// increment ref count before sending it via channel
			rec.Retain()
			streamChMap[targetStream] <- rec
		}
	}
}

func (c *DirectClient) Run(ctx context.Context) error {
	bqClient, err := bigquery.NewClient(ctx, c.projectID)
	if err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	defer bqClient.Close()
	mwClient, err := managedwriter.NewClient(ctx, c.projectID)
	if err != nil {
		return fmt.Errorf("Run: %w", err)
	}
	defer mwClient.Close()
	pipe, err := os.Open(c.pipeName)
	if err != nil {
		return err
	}
	defer pipe.Close()
	reader, err := ipc.NewReader(pipe)
	if err != nil {
		return fmt.Errorf("ipc.NewReader error: %w", err)
	}
	wg := &sync.WaitGroup{}
	errCh := make(chan error)
	streamChMap := make(map[string]chan arrow.Record)
	for _, name := range c.streamCatalog.GetStreamNames() {
		streamChMap[name] = make(chan arrow.Record, 100) // buffered chan for some backpressure management
		wg.Add(1)
		go run(ctx, wg, name, streamChMap[name], bqClient, mwClient, c, errCh)
	}
	wg.Add(1)
	go routeRecords(ctx, wg, reader, streamChMap, errCh)
	go func() {
		wg.Wait()
		close(errCh)
	}()
	if err, ok := <-errCh; ok {
		return fmt.Errorf("Run: %w", err)
	}
	return nil
}

func createWriteStream(ctx context.Context, mwClient *managedwriter.Client, projectID, datasetID, tableID, location string) (*storagepb.WriteStream, error) {
	pendingStream, err := mwClient.CreateWriteStream(ctx, &storagepb.CreateWriteStreamRequest{
		Parent: fmt.Sprintf("projects/%s/datasets/%s/tables/%s", projectID, datasetID, tableID),
		WriteStream: &storagepb.WriteStream{
			Type:     storagepb.WriteStream_PENDING,
			Location: location,
		},
	}, gax.WithRetry(func() gax.Retryer {
		return gax.OnCodes([]codes.Code{codes.NotFound}, gax.Backoff{}) // table creation is eventually consistent
	}))
	if err != nil {
		return nil, fmt.Errorf("createWriteStream: %w", err)
	}
	return pendingStream, nil
}

func createTable(ctx context.Context, schema *arrow.Schema, location string, tableRef *bigquery.Table, expirationTime time.Time) error {
	fields, err := arrowSchemaToBQSchema(schema)
	if err != nil {
		return fmt.Errorf("createTable: %w", err)
	}
	metaData := &bigquery.TableMetadata{
		Schema:         fields,
		ExpirationTime: expirationTime,
		Location:       location,
	}
	if err := tableRef.Create(ctx, metaData); err != nil {
		return fmt.Errorf("createTable: %w", err)
	}
	return nil
}

func tableExists(ctx context.Context, dataset *bigquery.Dataset, tableName string) bool {
	ts := dataset.Tables(ctx)
	for {
		t, err := ts.Next()
		if err == iterator.Done || err != nil {
			return false
		}
		if t.TableID == tableName {
			return true
		}
	}
}

func deleteTable(ctx context.Context, tableRef *bigquery.Table) error {
	if err := tableRef.Delete(ctx); err != nil {
		return fmt.Errorf("deleteTable: %w", err)
	}
	log.Println("successfully deleted tmp table")
	return nil
}

func arrowSchemaToBQSchema(schema *arrow.Schema) ([]*bigquery.FieldSchema, error) {
	fields := make([]*bigquery.FieldSchema, schema.NumFields()+arrowproto.Len-1)
	fields[0] = &bigquery.FieldSchema{Name: arrowproto.IDField, Type: bigquery.StringFieldType, Required: true}
	fields[1] = &bigquery.FieldSchema{Name: arrowproto.TimestampField, Type: bigquery.TimestampFieldType, Required: true}
	fields[2] = &bigquery.FieldSchema{Name: arrowproto.MetadataField, Type: bigquery.JSONFieldType, Required: true}
	for i := 0; i < schema.NumFields(); i++ {
		t, err := arrowTypeToBQType(schema.Field(i).Type.ID())
		if err != nil {
			return nil, fmt.Errorf("arrowSchemaToBQSchema: %s - %w", schema.Field(i).Name, err)
		}
		fields[i+arrowproto.Len-1] = &bigquery.FieldSchema{
			Name: schema.Field(i).Name,
			Type: t,
			// Required: schema.Field(i).Nullable,  // TODO: figure out a way to do this reliably
		}
	}
	return fields, nil
}

func arrowTypeToBQType(fieldType arrow.Type) (bigquery.FieldType, error) {
	switch fieldType {
	case arrow.STRING:
		return bigquery.StringFieldType, nil
	case arrow.TIMESTAMP:
		return bigquery.TimestampFieldType, nil
	case arrow.INT32:
		return bigquery.IntegerFieldType, nil
	case arrow.BOOL:
		return bigquery.BooleanFieldType, nil
	default:
		return bigquery.StringFieldType, fmt.Errorf("arrowTypeToBQType: unknown arrow type %s", fieldType.String())
	}
}

func createManagedStream(ctx context.Context, schema *arrow.Schema, streamName string, mwClient *managedwriter.Client, pendingStreamName string) (*managedwriter.ManagedStream, protoreflect.MessageDescriptor, error) {
	// We need to communicate the descriptor of the protocol buffer message we're using, which is analagous to the "schema" for the message.
	fd, err := arrowproto.GenerateDynamicProto(schema, streamName)
	if err != nil {
		return nil, nil, fmt.Errorf("createManagedStream: %w", err)
	}
	md, err := arrowproto.ConvertToMessageDescriptor(fd, streamName)
	if err != nil {
		return nil, nil, fmt.Errorf("createManagedStream: %w", err)
	}
	// we'll need this to serialize the batch
	descriptorProto, err := adapt.NormalizeDescriptor(md)
	if err != nil {
		return nil, nil, fmt.Errorf("createManagedStream: %w", err)
	}

	managedStream, err := mwClient.NewManagedStream(ctx, managedwriter.WithStreamName(pendingStreamName),
		managedwriter.WithSchemaDescriptor(descriptorProto))
	if err != nil {
		return nil, nil, fmt.Errorf("createManagedStream: %w", err)
	}
	return managedStream, md, nil
}

func finalizeManagedStream(ctx context.Context, managedStream *managedwriter.ManagedStream) error {
	// We're now done appending to this stream.  We now mark pending stream finalized, which blocks
	// further appends.
	rowCount, err := managedStream.Finalize(ctx)
	if err != nil {
		return fmt.Errorf("finalizeManagedStream: %w", err)
	}
	log.Printf("Stream %s finalized with %d rows.\n", managedStream.StreamName(), rowCount)
	return nil
}

func commitWriteStream(ctx context.Context, managedStream *managedwriter.ManagedStream, mwClient *managedwriter.Client) error {
	// To commit the data to the table, we need to run a batch commit.  You can commit several streams
	// atomically as a group, but in this instance we'll only commit the single stream.
	req := &storagepb.BatchCommitWriteStreamsRequest{
		Parent:       managedwriter.TableParentFromStreamName(managedStream.StreamName()),
		WriteStreams: []string{managedStream.StreamName()},
	}

	resp, err := mwClient.BatchCommitWriteStreams(ctx, req)
	if err != nil {
		return fmt.Errorf("commitWriteStream - BatchCommit: %w", err)
	}
	if len(resp.GetStreamErrors()) > 0 {
		return fmt.Errorf("commitWriteStream - stream errors present: %v", resp.GetStreamErrors())
	}

	fmt.Printf("%s data committed at %s\n", managedStream.StreamName(), resp.GetCommitTime().AsTime().Format(time.RFC3339Nano))
	return nil
}

func checkWrite(ctx context.Context, wg *sync.WaitGroup, results <-chan *writeContext, errCh chan<- error, barrier *writeBarrier) {
	defer wg.Done()
	for result := range results {
		select {
		case <-ctx.Done():
			return
		default:
			// Now, we'll check that our batch of appends all completed successfully.
			res, err := result.result.FullResponse(ctx)
			for _, e := range res.GetRowErrors() {
				log.Println(e.Code, e.Message)
			}
			if err != nil {
				log.Printf("checkWrite - append returned error on offset %d: %s\n", result.state.curOffset, err.Error())
				log.Println("Retrying...")
				// TODO: implement retry
				barrier.lock()
				noopRetry()
				barrier.unlock()
				errCh <- fmt.Errorf("checkWrite - append returned error on offset %d: %w", result.state.curOffset, err)
				return
			}
			recvOffset := res.GetAppendResult().GetOffset().Value
			if recvOffset != result.state.curOffset {
				errCh <- fmt.Errorf("checkWrite: received offset does not match sent offset")
				return
			}
			log.Printf("Successfully appended data at offset %d.\n", recvOffset)
			// release memory once the write is confirmed
			result.batch.Release()
		}
	}
}

func noopRetry() {}

func write(ctx context.Context, state *streamState, batch arrow.Record, results chan<- *writeContext) error {
	rows, err := arrowproto.SerializeData(state.schema, batch, state.msgDesc)
	if err != nil {
		return nil
	}
	result, err := state.managedStream.AppendRows(ctx, rows, managedwriter.WithOffset(state.curOffset))
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	// retain memory for downstream verification
	batch.Retain()
	results <- &writeContext{
		result: result,
		batch:  batch,
		state:  *state,
	}
	return nil
}
