package gocbcore

import (
	"encoding/json"
	"strconv"
)

type kvErrorMapAttribute string

type kvErrorMapError struct {
	Name        string
	Description string
	Attributes  []kvErrorMapAttribute
}

type kvErrorMap struct {
	Version  int
	Revision int
	Errors   map[uint16]kvErrorMapError
}

type cfgKvErrorMapError struct {
	Name  string   `json:"name"`
	Desc  string   `json:"desc"`
	Attrs []string `json:"attrs"`
}

type cfgKvErrorMap struct {
	Version  int `json:"version"`
	Revision int `json:"revision"`
	Errors   map[string]cfgKvErrorMapError
}

func parseKvErrorMap(data []byte) (*kvErrorMap, error) {
	var cfg cfgKvErrorMap
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	var errMap kvErrorMap
	errMap.Version = cfg.Version
	errMap.Revision = cfg.Revision
	errMap.Errors = make(map[uint16]kvErrorMapError)
	for errCodeStr, errData := range cfg.Errors {
		errCode, err := strconv.ParseInt(errCodeStr, 16, 64)
		if err != nil {
			return nil, err
		}

		var errInfo kvErrorMapError
		errInfo.Name = errData.Name
		errInfo.Description = errData.Desc
		errInfo.Attributes = make([]kvErrorMapAttribute, len(errData.Attrs))
		for i, attr := range errData.Attrs {
			errInfo.Attributes[i] = kvErrorMapAttribute(attr)
		}
		errMap.Errors[uint16(errCode)] = errInfo
	}

	return &errMap, nil
}
