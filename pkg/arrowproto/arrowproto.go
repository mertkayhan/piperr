package arrowproto

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/google/uuid"
	"github.com/mertkayhan/piperr/pkg/utils"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	IDField        = "_ID"
	TimestampField = "_IngestionTimestamp"
	MetadataField  = "_Metadata"
)

const (
	IDIndex = iota + 1
	TimestampIndex
	MetadataIndex
	Len
)

func GenerateDynamicProto(schema *arrow.Schema, streamName string) (*descriptorpb.FileDescriptorProto, error) {
	// Create a new FileDescriptorProto
	fileDescProto := &descriptorpb.FileDescriptorProto{
		Name: proto.String(fmt.Sprintf("%s.proto", strings.ToLower(streamName))),
	}

	// Add a message descriptor
	msgDesc := &descriptorpb.DescriptorProto{
		Name: proto.String(streamName),
	}

	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64
	id := &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(IDField),
		Type:     &stringType,
		Number:   proto.Int32(IDIndex),
		JsonName: proto.String(IDField),
	}
	ingestionTime := &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(TimestampField),
		Type:     &int64Type,
		Number:   proto.Int32(TimestampIndex),
		JsonName: proto.String(TimestampField),
	}
	metadata := &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(MetadataField),
		Type:     &stringType,
		Number:   proto.Int32(MetadataIndex),
		JsonName: proto.String(MetadataField),
	}
	msgDesc.Field = append(msgDesc.Field, id, ingestionTime, metadata)

	// Map Arrow schema to Protobuf schema
	for i, field := range schema.Fields() {
		t, err := mapArrowTypeToProto(field.Type)
		if err != nil {
			return nil, fmt.Errorf("GenerateDynamicProto: %w", err)
		}
		fieldDesc := &descriptorpb.FieldDescriptorProto{
			Name: proto.String(utils.SanitizeFieldName(field.Name)),
			// Map Arrow type to Protobuf type
			Type:     t,
			Number:   proto.Int32(int32(i + Len)), // offset by len
			JsonName: proto.String(utils.SanitizeFieldName(field.Name)),
			// Proto3Optional: proto.Bool(true),
		}
		msgDesc.Field = append(msgDesc.Field, fieldDesc)
	}

	fileDescProto.MessageType = append(fileDescProto.MessageType, msgDesc)
	return fileDescProto, nil
}

func mapArrowTypeToProto(arrowType arrow.DataType) (*descriptorpb.FieldDescriptorProto_Type, error) {
	// Add mappings for Arrow types to Protobuf types
	switch arrowType.ID() {
	case arrow.INT32:
		t := descriptorpb.FieldDescriptorProto_TYPE_INT32
		return &t, nil
	case arrow.STRING:
		t := descriptorpb.FieldDescriptorProto_TYPE_STRING
		return &t, nil
	case arrow.TIMESTAMP:
		t := descriptorpb.FieldDescriptorProto_TYPE_INT64
		return &t, nil
	case arrow.BOOL:
		t := descriptorpb.FieldDescriptorProto_TYPE_BOOL
		return &t, nil
	// Add more mappings as needed
	default:
		return nil, fmt.Errorf("mapArrowTypeToProto: unknown arrow type %s", arrowType.Name())
	}
}

func SerializeData(schema *arrow.Schema, record arrow.Record, msgDesc protoreflect.MessageDescriptor) ([][]byte, error) {
	serializedData := make([][]byte, record.NumRows())
	idField := msgDesc.Fields().ByNumber(protoreflect.FieldNumber(IDIndex))
	timestamp := time.Now().UnixNano() / 1000
	timestampField := msgDesc.Fields().ByNumber(protoreflect.FieldNumber(TimestampIndex))
	metadataBytes, err := json.Marshal(schema.Metadata().ToMap())
	if err != nil {
		return nil, fmt.Errorf("SerializeData: %w", err)
	}
	metadata := string(metadataBytes)
	metadataField := msgDesc.Fields().ByNumber(protoreflect.FieldNumber(MetadataIndex))
	for rowIdx := 0; rowIdx < int(record.NumRows()); rowIdx++ {
		dynamicMsg := dynamicpb.NewMessage(msgDesc)
		dynamicMsg.Set(idField, protoreflect.ValueOfString(uuid.New().String()))
		dynamicMsg.Set(timestampField, protoreflect.ValueOfInt64(timestamp))
		dynamicMsg.Set(metadataField, protoreflect.ValueOfString(metadata))
		for colIdx, col := range record.Columns() {
			field := msgDesc.Fields().ByNumber(protoreflect.FieldNumber(colIdx + Len))
			if err := setField(col, dynamicMsg, field, rowIdx); err != nil {
				return nil, fmt.Errorf("SerializeData: %w", err)
			}
		}
		// Serialize the dynamic message
		serializedRow, err := proto.Marshal(dynamicMsg)
		if err != nil {
			return nil, fmt.Errorf("SerializeData: %w", err)
		}
		serializedData[rowIdx] = serializedRow
	}

	return serializedData, nil
}

func setField(col arrow.Array, msg *dynamicpb.Message, field protoreflect.FieldDescriptor, i int) error {
	switch col.DataType().ID() {
	case arrow.INT32:
		arr := col.(*array.Int32)
		if !arr.IsNull(i) {
			value := arr.Value(i)
			msg.Set(field, protoreflect.ValueOfInt32(value))
		}
	case arrow.STRING:
		arr := col.(*array.String)
		if !arr.IsNull(i) {
			value := arr.Value(i)
			msg.Set(field, protoreflect.ValueOfString(value))
		}
	case arrow.TIMESTAMP:
		arr := col.(*array.Timestamp)
		if !arr.IsNull(i) {
			value := arr.Value(i).ToTime(3).UnixNano() / 1000
			msg.Set(field, protoreflect.ValueOfInt64(value))
		}
	case arrow.BOOL:
		arr := col.(*array.Boolean)
		if !arr.IsNull(i) {
			value := arr.Value(i)
			msg.Set(field, protoreflect.ValueOfBool(value))
		}
	default:
		return fmt.Errorf("setField: unknown arrow type %s", col.DataType().Name())
	}
	return nil
}

func ConvertToMessageDescriptor(fileDescProto *descriptorpb.FileDescriptorProto, streamName string) (protoreflect.MessageDescriptor, error) {
	fileDesc, err := protodesc.NewFile(fileDescProto, &protoregistry.Files{})
	if err != nil {
		return nil, fmt.Errorf("ConvertToMessageDescriptor: %w", err)
	}

	messageDesc := fileDesc.Messages().ByName(protoreflect.Name(streamName))
	if messageDesc == nil {
		return nil, fmt.Errorf("ConvertToMessageDescriptor: message %s not found", streamName)
	}
	return messageDesc, nil
}
