-- +goose Up
-- Registers Resend as the active email provider so
-- ListActiveProvidersByType('email') returns it.
--
-- The API key deliberately stays out of this row: migrations are committed,
-- so the key keeps coming from RESEND_API_KEY in the environment (the
-- consumer builds resend.Config from env, not from this JSONB - see
-- internal/app/consumer.go). The row records the from address and the
-- selection, not the secret.
INSERT INTO providers (name, type, config, priority, is_active)
VALUES (
           'resend',
           'email',
           jsonb_build_object(
                   'from', 'disillusioned-labs@gtfobae.my.id'
               ),
           1,
           true
       )
ON CONFLICT (name) DO NOTHING;


-- +goose Down
DELETE FROM providers
WHERE name = 'resend';
