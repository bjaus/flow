package provider

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/bjaus/flow/app/internal/core"
	"github.com/cloudwego/eino-ext/components/model/openai"
	"github.com/cloudwego/eino/components/model"
)

type GatewayProvider struct {
	baseURL string
	timeout time.Duration
}

func NewGateway(baseURL string) *GatewayProvider {
	return &GatewayProvider{baseURL: strings.TrimRight(baseURL, "/"), timeout: 2 * time.Minute}
}

func (p *GatewayProvider) Model(ctx context.Context, persona core.Persona) (model.BaseChatModel, error) {
	baseURL := p.baseURL
	if baseURL == "" {
		baseURL = strings.TrimRight(os.Getenv("FLOW_GATEWAY_URL"), "/")
	}
	if baseURL == "" {
		return nil, errors.New("FLOW_GATEWAY_URL is not set")
	}
	key := os.Getenv("FLOW_GATEWAY_KEY")
	if key == "" {
		key = os.Getenv("OPENAI_API_KEY")
	}
	if key == "" {
		key = "local"
	}
	if persona.Model == "" {
		return nil, errors.New("persona model is required")
	}
	return openai.NewChatModel(ctx, &openai.ChatModelConfig{
		APIKey:              key,
		BaseURL:             baseURL,
		Model:               persona.Model,
		Timeout:             p.timeout,
		Temperature:         persona.Temperature,
		TopP:                persona.TopP,
		MaxCompletionTokens: persona.MaxCompletionTokens,
		Stop:                persona.Stop,
		PresencePenalty:     persona.PresencePenalty,
		FrequencyPenalty:    persona.FrequencyPenalty,
		Seed:                persona.Seed,
	})
}
