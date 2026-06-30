package serializer

import (
	"bytes"
	"encoding/json"
)

type SerializeFunc func(any) ([]byte, error)
type DeserializeFunc func([]byte, any) error

type Encoder struct{}

func NewJSONSerializer() *Encoder {
	return &Encoder{}
}

func (e *Encoder) Serialize(v any) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func (e *Encoder) Deserialize(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (e *Encoder) EncodeDataToString(v any) (string, error) {
	data, err := e.Serialize(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (e *Encoder) DecodeStringToData(data string, v any) error {
	return e.Deserialize([]byte(data), v)
}
