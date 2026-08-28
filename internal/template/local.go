package template

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"path"
	"strings"
	texttemplate "text/template"
)

var (
	//go:embed templates/*
	templateFS embed.FS
)

const (
	emailTemplateRoot = "templates/email"

	subjectTemplateName = "subject.tmpl"
	htmlTemplateName    = "body.html.tmpl"
	textTemplateName    = "body.text.tmpl"
)

type LocalRenderer struct {
	emailTemplates  map[string]*emailTemplate
	payloadRegistry *PayloadRegistry
}

type emailTemplate struct {
	subject *texttemplate.Template
	html    *template.Template
	text    *texttemplate.Template
}

func NewLocalRenderer(
	payloadRegistry *PayloadRegistry,
) (*LocalRenderer, error) {
	if payloadRegistry == nil {
		return nil, fmt.Errorf(
			"payload registry is required",
		)
	}

	r := &LocalRenderer{
		emailTemplates:  make(map[string]*emailTemplate),
		payloadRegistry: payloadRegistry,
	}

	if err := r.loadEmailTemplates(); err != nil {
		return nil, fmt.Errorf(
			"load email templates: %w",
			err,
		)
	}

	if err := r.validateRegistrations(); err != nil {
		return nil, fmt.Errorf(
			"validate template registrations: %w",
			err,
		)
	}

	return r, nil
}

func (r *LocalRenderer) Render(
	ctx context.Context,
	notificationType string,
	channel string,
	payload []byte,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	switch channel {
	case "email":
		return r.renderEmail(
			ctx,
			notificationType,
			payload,
		)

	default:
		return nil, fmt.Errorf(
			"unsupported notification channel %q",
			channel,
		)
	}
}

func (r *LocalRenderer) loadEmailTemplates() error {
	entries, err := fs.ReadDir(
		templateFS,
		emailTemplateRoot,
	)
	if err != nil {
		return fmt.Errorf(
			"read email template directory: %w",
			err,
		)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		notificationType := entry.Name()

		if err := r.loadEmailTemplate(notificationType); err != nil {
			return err
		}
	}

	if len(r.emailTemplates) == 0 {
		return fmt.Errorf(
			"no email templates found in %q",
			emailTemplateRoot,
		)
	}

	return nil
}

func (r *LocalRenderer) loadEmailTemplate(
	notificationType string,
) error {
	basePath := path.Join(
		emailTemplateRoot,
		notificationType,
	)

	subjectPath := path.Join(
		basePath,
		subjectTemplateName,
	)

	htmlPath := path.Join(
		basePath,
		htmlTemplateName,
	)

	textPath := path.Join(
		basePath,
		textTemplateName,
	)

	subject, err := texttemplate.ParseFS(
		templateFS,
		subjectPath,
	)
	if err != nil {
		return fmt.Errorf(
			"parse email template %q subject: %w",
			notificationType,
			err,
		)
	}

	html, err := template.ParseFS(
		templateFS,
		htmlPath,
	)
	if err != nil {
		return fmt.Errorf(
			"parse email template %q html: %w",
			notificationType,
			err,
		)
	}

	text, err := texttemplate.ParseFS(
		templateFS,
		textPath,
	)
	if err != nil {
		return fmt.Errorf(
			"parse email template %q text: %w",
			notificationType,
			err,
		)
	}

	r.emailTemplates[notificationType] = &emailTemplate{
		subject: subject,
		html:    html,
		text:    text,
	}

	return nil
}

func (r *LocalRenderer) validateRegistrations() error {
	for _, notificationType := range r.payloadRegistry.Types() {
		if _, ok := r.emailTemplates[notificationType]; !ok {
			return fmt.Errorf(
				"payload registered for notification type %q but email template is missing",
				notificationType,
			)
		}
	}

	for notificationType := range r.emailTemplates {
		if _, ok := r.payloadRegistry.New(notificationType); !ok {
			return fmt.Errorf(
				"email template exists for notification type %q but payload is not registered",
				notificationType,
			)
		}
	}

	return nil
}

func (r *LocalRenderer) renderEmail(
	ctx context.Context,
	notificationType string,
	payload []byte,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tmpl, ok := r.emailTemplates[notificationType]
	if !ok {
		return nil, fmt.Errorf(
			"email template not found for notification type %q",
			notificationType,
		)
	}

	data, ok := r.payloadRegistry.New(notificationType)
	if !ok {
		return nil, fmt.Errorf(
			"payload schema not found for notification type %q",
			notificationType,
		)
	}

	if err := json.Unmarshal(payload, data); err != nil {
		return nil, fmt.Errorf(
			"decode %s payload: %w",
			notificationType,
			err,
		)
	}

	if err := data.Validate(); err != nil {
		return nil, fmt.Errorf(
			"validate %s payload: %w",
			notificationType,
			err,
		)
	}

	var subject bytes.Buffer

	if err := tmpl.subject.Execute(&subject, data); err != nil {
		return nil, fmt.Errorf(
			"render %s email subject: %w",
			notificationType,
			err,
		)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var html bytes.Buffer

	if err := tmpl.html.Execute(&html, data); err != nil {
		return nil, fmt.Errorf(
			"render %s email html: %w",
			notificationType,
			err,
		)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var text bytes.Buffer

	if err := tmpl.text.Execute(&text, data); err != nil {
		return nil, fmt.Errorf(
			"render %s email text: %w",
			notificationType,
			err,
		)
	}

	rendered := struct {
		Subject string `json:"subject"`
		HTML    string `json:"html,omitempty"`
		Text    string `json:"text,omitempty"`
	}{
		Subject: strings.TrimSpace(subject.String()),
		HTML:    html.String(),
		Text:    text.String(),
	}

	result, err := json.Marshal(rendered)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal rendered email payload: %w",
			err,
		)
	}

	return result, nil
}
