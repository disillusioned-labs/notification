package template

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
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
	pushTemplateRoot  = "templates/push"

	subjectTemplateName = "subject.tmpl"
	htmlTemplateName    = "body.html.tmpl"
	textTemplateName    = "body.text.tmpl"
	titleTemplateName   = "title.tmpl"
	bodyTemplateName    = "body.tmpl"
)

type LocalRenderer struct {
	emailTemplates  map[string]*emailTemplate
	pushTemplates   map[string]*pushTemplate
	payloadRegistry *PayloadRegistry
}

type emailTemplate struct {
	subject *texttemplate.Template
	html    *template.Template
	text    *texttemplate.Template
}

// pushTemplate renders a device notification as title + body. Not every
// notification type supports push - a push delivery for a type without a
// template fails at render time, which is the producer's contract error
// surfacing in the delivery attempt record.
type pushTemplate struct {
	title *texttemplate.Template
	body  *texttemplate.Template
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
		pushTemplates:   make(map[string]*pushTemplate),
		payloadRegistry: payloadRegistry,
	}

	if err := r.loadEmailTemplates(); err != nil {
		return nil, fmt.Errorf(
			"load email templates: %w",
			err,
		)
	}

	if err := r.loadPushTemplates(); err != nil {
		return nil, fmt.Errorf(
			"load push templates: %w",
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

	case "push":
		return r.renderPush(
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

func (r *LocalRenderer) loadPushTemplates() error {
	entries, err := fs.ReadDir(
		templateFS,
		pushTemplateRoot,
	)
	if err != nil {
		// The push directory is optional: a type without push templates
		// simply does not support the push channel.
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read push template directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		notificationType := entry.Name()

		if err := r.loadPushTemplate(notificationType); err != nil {
			return err
		}
	}

	return nil
}

func (r *LocalRenderer) loadPushTemplate(
	notificationType string,
) error {
	basePath := path.Join(
		pushTemplateRoot,
		notificationType,
	)

	title, err := texttemplate.ParseFS(
		templateFS,
		path.Join(basePath, titleTemplateName),
	)
	if err != nil {
		return fmt.Errorf(
			"parse push template %q title: %w",
			notificationType,
			err,
		)
	}

	body, err := texttemplate.ParseFS(
		templateFS,
		path.Join(basePath, bodyTemplateName),
	)
	if err != nil {
		return fmt.Errorf(
			"parse push template %q body: %w",
			notificationType,
			err,
		)
	}

	r.pushTemplates[notificationType] = &pushTemplate{
		title: title,
		body:  body,
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

	// Push templates are a subset: each one must reuse a registered payload
	// schema, but a payload is free to be email-only.
	for notificationType := range r.pushTemplates {
		if _, ok := r.payloadRegistry.New(notificationType); !ok {
			return fmt.Errorf(
				"push template exists for notification type %q but payload is not registered",
				notificationType,
			)
		}
	}

	return nil
}

// pushData extracts the machine-readable routing map a client uses to decide
// what to open on tap. Payload types that reference a transaction implement
// transactionReferrer.
type transactionReferrer interface {
	TransactionRef() string
}

func pushData(notificationType string, payload Payload) map[string]string {
	data := map[string]string{
		"notification_type": notificationType,
	}

	if ref, ok := payload.(transactionReferrer); ok {
		if id := ref.TransactionRef(); id != "" {
			data["transaction_id"] = id
		}
	}

	return data
}

// renderPush produces {"title","body","data"} for the FCM provider. The data
// map is fixed: notification type and transaction reference, so a client can
// route a tap without parsing human-readable text.
func (r *LocalRenderer) renderPush(
	ctx context.Context,
	notificationType string,
	payload []byte,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	tmpl, ok := r.pushTemplates[notificationType]
	if !ok {
		return nil, fmt.Errorf(
			"push template not found for notification type %q",
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

	var title bytes.Buffer

	if err := tmpl.title.Execute(&title, data); err != nil {
		return nil, fmt.Errorf(
			"render %s push title: %w",
			notificationType,
			err,
		)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var body bytes.Buffer

	if err := tmpl.body.Execute(&body, data); err != nil {
		return nil, fmt.Errorf(
			"render %s push body: %w",
			notificationType,
			err,
		)
	}

	rendered := struct {
		Title string            `json:"title"`
		Body  string            `json:"body"`
		Data  map[string]string `json:"data,omitempty"`
	}{
		Title: strings.TrimSpace(title.String()),
		Body:  strings.TrimSpace(body.String()),
		Data:  pushData(notificationType, data),
	}

	result, err := json.Marshal(rendered)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal rendered push payload: %w",
			err,
		)
	}

	return result, nil
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
