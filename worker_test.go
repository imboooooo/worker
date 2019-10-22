package worker

import (
	"sync"
	"testing"
)

func TestNewWorker(t *testing.T) {
	type args struct {
		numberWorker int
		fn           func(string) error
	}
	tests := []struct {
		name string
		args args
		want *Worker
	}{
		{
			name: "success",
			args: struct {
				numberWorker int
				fn           func(string) error
			}{numberWorker: 1, fn: func(s string) error {
				return nil
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewWorker(tt.args.numberWorker, tt.args.fn)
			if got == nil {
				t.Errorf("NewWorker() = %v, want %v", got, tt.want)
			}
			got.SendJob(tt.name)
			got.Stop()
		})
	}
}

func TestWorker_SendJob(t *testing.T) {
	type fields struct {
		signal chan struct{}
		msg    []message
		muMSG  sync.Mutex
		fn     func(string) error
		wg     *sync.WaitGroup
	}
	type args struct {
		payload string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "success",
			fields: fields{
				signal: make(chan struct{}),
				msg:    make([]message, 10),
				fn: func(s string) error {
					return nil
				},
				wg: new(sync.WaitGroup),
			},
			args: struct{ payload string }{payload: "success"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				signal: tt.fields.signal,
				msg:    tt.fields.msg,
				muMSG:  tt.fields.muMSG,
				fn:     tt.fields.fn,
				wg:     tt.fields.wg,
			}
			w.SendJob(tt.args.payload)
		})
	}
}
