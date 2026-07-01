package pkg

import "encoding/json"

func JSONMarshalIndent(v any) ([]byte, error) { return json.MarshalIndent(v, "", "  ") }
