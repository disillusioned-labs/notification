package constant

// Notification categories mirror the CHECK constraint on
// notifications.category and select the Kafka lane a delivery event is
// published to.
const (
	CategoryTransactional = "transactional"
	CategorySocial        = "social"
	CategoryMarketing     = "marketing"
)
