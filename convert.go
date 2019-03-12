package numbersix

import (
	"encoding/json"
)

func marshal(v interface{}) (string, error) {
	m, err := json.Marshal(v)

	return string(m), err
}

func unmarshal(data string, v interface{}) error {
	return json.Unmarshal([]byte(data), &v)
}
