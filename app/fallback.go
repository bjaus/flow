package app

import (
	"context"
	"errors"
	"io"

	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

type fallbackModel struct{ models []model.BaseChatModel }

func (f fallbackModel) Generate(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	var last error
	for _, candidate := range f.models {
		out, err := candidate.Generate(ctx, messages, opts...)
		if err == nil {
			return out, nil
		}
		last = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, last
}
func (f fallbackModel) Stream(ctx context.Context, messages []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	reader, writer := schema.Pipe[*schema.Message](16)
	go func() {
		defer writer.Close()
		var last error
		for _, candidate := range f.models {
			stream, err := candidate.Stream(ctx, messages, opts...)
			if err != nil {
				last = err
				continue
			}
			sent := false
			for {
				chunk, recvErr := stream.Recv()
				if recvErr == nil {
					sent = true
					writer.Send(chunk, nil)
					continue
				}
				stream.Close()
				if errors.Is(recvErr, io.EOF) {
					return
				}
				last = recvErr
				if sent {
					writer.Send(nil, recvErr)
					return
				}
				break
			}
			if ctx.Err() != nil {
				writer.Send(nil, ctx.Err())
				return
			}
		}
		writer.Send(nil, last)
	}()
	return reader, nil
}
func (f fallbackModel) WithTools(tools []*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	bound := make([]model.BaseChatModel, 0, len(f.models))
	for _, candidate := range f.models {
		toolModel, ok := candidate.(model.ToolCallingChatModel)
		if !ok {
			return nil, errors.New("fallback model does not support tools")
		}
		configured, err := toolModel.WithTools(tools)
		if err != nil {
			return nil, err
		}
		bound = append(bound, configured)
	}
	return fallbackModel{models: bound}, nil
}
