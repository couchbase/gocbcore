package jcbmock

import (
	"encoding/json"
	"log"
	"strings"
)

type command struct {
	Code CmdCode
	Body map[string]interface{}
}

func (c command) Encode() (encoded []byte) {
	payload := make(map[string]interface{})
	payload["command"] = c.Code
	if c.Body != nil {
		payload["payload"] = c.Body
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic("Received invalid command for marshal")
	}
	return
}

func (c command) Set(key string, value interface{}) {
	c.Body[key] = value
}

// Command is used to specify a command to run.
type Command interface {
	Encode() []byte
	Set(key string, value interface{})
}

// Response is the result of running a command.
type Response struct {
	Payload map[string]interface{}
}

// Success returns whether or not the command was successful.
func (r *Response) Success() bool {
	s, exists := r.Payload["status"]
	if !exists {
		log.Print("Warning: status field not found!")
		return false
	}

	b, castok := s.(string)
	if !castok {
		log.Print("Bad type in 'status'")
		return false
	}
	return strings.ToLower(b)[0] == 'o'
}

// NewCommand returns a new command for a given command code and body.
func NewCommand(code CmdCode, body map[string]interface{}) Command {
	return command{Code: code, Body: body}
}
