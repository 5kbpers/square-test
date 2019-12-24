package test

var (
	CreateCustomersSQL = "CREATE TABLE IF NOT EXISTS customers (" +
		"id BIGINT NOT NULL AUTO_INCREMENT," +
		"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
		"updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
		"name TEXT NOT NULL," +
		"PRIMARY KEY (id)" +
		");"

	CreateMovementsSQL = "CREATE TABLE IF NOT EXISTS movements (" +
		"id BIGINT NOT NULL AUTO_INCREMENT," +
		"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
		"updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
		"payment_id BIGINT NOT NULL," +
		"customer_id BIGINT NOT NULL," +
		"counterparty_id BIGINT NOT NULL," +
		"role VARCHAR(20) NOT NULL," +
		"amount_cents BIGINT NOT NULL," +
		"PRIMARY KEY (id)" +
		");" +
		"ALTER TABLE movements ADD INDEX idx_customer_id (customer_id);" +
		"ALTER TABLE movements ADD INDEX idx_created_at (role, created_at);"
)

var (
	InsertCustomerSQL = "INSERT INTO customers (id, name) VALUES (?, ?);"
	InsertMovementSQL = "INSERT INTO movements (role, customer_id, counterparty_id, payment_id, amount_cents) VALUES (?, ?, ?, ?, ?);"

	QueryCustomerMovementsSQL = "SELECT * FROM movements WHERE customer_id = ? ORDER BY created_at desc LIMIT 100;"
	QueryAllMovementsSQL = "SELECT * FROM movements WHERE role = 'SENDER' ORDER BY created_at desc LIMIT 100;"
)
