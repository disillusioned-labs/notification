package template

import "context"

type Renderer interface {
	Render(
		ctx context.Context,
		notificationType string,
		channel string,
		payload []byte,
	) ([]byte, error)
}
