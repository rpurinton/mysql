<?php

declare(strict_types=1);

namespace RPurinton;

use mysqli;
use mysqli_result;
use RPurinton\Config;
use RPurinton\Validators\MySQLValidators;
use RPurinton\Exceptions\MySQLException;

/**
 * MySQL database abstraction layer.
 */
class MySQL
{
    private ?mysqli $sql = null;
    private int $ping_time = 0;
    private int $wait_timeout = 0;

    /**
     * Constructor.
     *
     * @param array|null $config Optional configuration overrides.
     * @throws MySQLException When initialization fails.
     */
    public function __construct(private ?array $config = null)
    {
        try {
            $this->config = Config::get('MySQL', [
                'host' => MySQLValidators::validateDb(...),
                'user' => MySQLValidators::validateUser(...),
                'pass' => MySQLValidators::validatePass(...),
                'db'   => MySQLValidators::validateDb(...),
            ], $config);
            $this->reconnect();
            register_shutdown_function($this->shutdown(...));
        } catch (\Throwable $e) {
            throw new MySQLException('Initialization failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Creates a new MySQL instance and connects to the database.
     *
     * @param array|null $config Optional configuration overrides.
     * @return self
     * @throws MySQLException On connection failure.
     */
    public static function connect(?array $config = null): self
    {
        return new self($config);
    }

    /**
     * Re-establishes a connection to the database.
     *
     * @return void
     * @throws MySQLException On connection failure.
     */
    public function reconnect(): void
    {
        try {
            if ($this->sql) {
                $this->sql->close();
            }
            extract($this->config);
            $this->sql = new mysqli($host, $user, $pass, $db);
            if ($this->sql->connect_error) {
                throw new MySQLException(
                    'Connect Error (' . $this->sql->connect_errno . ') ' . $this->sql->connect_error
                );
            }
            if (!$this->sql->set_charset('utf8mb4')) {
                throw new MySQLException('Error setting charset: ' . $this->sql->error);
            }
            $this->wait_timeout = (int)$this->fetch_one('SELECT @@wait_timeout');
            $this->ping_time = time();
        } catch (\Throwable $e) {
            throw new MySQLException('Reconnect failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Checks the database connection and reconnects if necessary.
     *
     * Note: This is a custom ping function because mysqli_ping is deprecated.
     *
     * @return void
     * @throws MySQLException On query or reconnection failure.
     */
    public function ping(): void
    {
        if (time() - $this->ping_time < $this->wait_timeout) {
            $this->ping_time = time();
            return;
        }
        try {
            $this->sql->query('SELECT 1');
            $this->ping_time = time();
        } catch (\Throwable $e) {
            $this->reconnect();
        }
    }

    /**
     * Executes a query and returns a result set or null for non-result queries.
     *
     * @param string $query The SQL query to execute.
     * @return mysqli_result|null
     * @throws MySQLException On query failure.
     */
    public function query(string $query): ?mysqli_result
    {
        try {
            $this->ping();
            $result = $this->sql->query($query);
            if ($result === false) {
                throw new MySQLException('Query Error: ' . $this->sql->error);
            }
            return $result === true ? null : $result;
        } catch (\Throwable $e) {
            throw new MySQLException('Query failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Executes a SELECT query and returns all rows as an associative array.
     *
     * @param string $query The SELECT query.
     * @return array|null
     * @throws MySQLException On query failure.
     */
    public function fetch_all(string $query): ?array
    {
        try {
            $result = $this->query($query);
            return $result->fetch_all(MYSQLI_ASSOC);
        } catch (\Throwable $e) {
            throw new MySQLException('Fetch all failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Executes a SELECT query and returns the first row as an associative array.
     *
     * @param string $query The SELECT query.
     * @return array|null
     * @throws MySQLException On query failure.
     */
    public function fetch_row(string $query): ?array
    {
        try {
            $result = $this->query($query);
            return $result->fetch_assoc();
        } catch (\Throwable $e) {
            throw new MySQLException('Fetch row failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Executes a SELECT query and returns the first column of the first row.
     *
     * @param string $query The SELECT query.
     * @return mixed
     * @throws MySQLException On query failure.
     */
    public function fetch_one(string $query): mixed
    {
        try {
            $result = $this->query($query);
            if (!$result) {
                return null;
            }
            $row = $result->fetch_row();
            return (is_array($row) && isset($row[0])) ? $row[0] : null;
        } catch (\Throwable $e) {
            throw new MySQLException('Fetch one failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Executes a SELECT query and returns the first column of each row.
     *
     * @param string $query The SELECT query.
     * @return array|null
     * @throws MySQLException On query failure.
     */
    public function fetch_column(string $query): ?array
    {
        try {
            $result = $this->query($query);
            $column = [];
            while ($row = $result->fetch_row()) {
                $column[] = $row[0];
            }
            return $column;
        } catch (\Throwable $e) {
            throw new MySQLException('Fetch column failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Executes multiple queries in a single call.
     *
     * @param string $query The SQL queries to execute.
     * @return array|null Array of result sets.
     * @throws MySQLException On execution failure.
     */
    public function multi(string $query): ?array
    {
        try {
            $this->ping();
            if (!$this->sql->multi_query($query)) {
                throw new MySQLException('Multi Query Error: ' . $this->sql->error);
            }
            $results = [];
            do {
                $result = $this->sql->store_result();
                if ($result) {
                    $results[] = $result->fetch_all(MYSQLI_ASSOC);
                    $result->free();
                }
            } while ($this->sql->more_results() && $this->sql->next_result());
            return $results;
        } catch (\Throwable $e) {
            throw new MySQLException('Multi query failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Executes an INSERT statement and returns the new row ID.
     *
     * @param string $query The INSERT query.
     * @return int|string Insert ID.
     * @throws MySQLException On query failure.
     */
    public function insert(string $query): int|string
    {
        try {
            $this->query($query);
            return $this->sql->insert_id;
        } catch (\Throwable $e) {
            throw new MySQLException('Insert failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Escapes a string or array of strings for safe query execution.
     *
     * @param string|array $input The input to escape.
     * @return string|array
     * @throws MySQLException On escape failure.
     */
    public function escape(string|array $input): string|array
    {
        try {
            if (is_array($input)) {
                return array_map([$this, 'escape'], $input);
            }
            return $this->sql->real_escape_string($input);
        } catch (\Throwable $e) {
            throw new MySQLException('Escape failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Returns the ID generated by the most recent INSERT.
     *
     * @return int|string
     */
    public function last_insert_id(): int|string
    {
        return $this->sql->insert_id;
    }

    /**
     * Returns the number of rows affected by the last query.
     *
     * @return int
     */
    public function affected_rows(): int
    {
        return $this->sql->affected_rows;
    }

    /**
     * Prepares and executes a parameterized query.
     *
     * @param string $query The SQL query with placeholders.
     * @param array $params Parameters to bind.
     * @return mysqli_result|null
     * @throws MySQLException On prepare, binding, or execution failure.
     */
    public function prepareAndExecute(string $query, array $params = []): ?mysqli_result
    {
        try {
            $this->ping();
            $stmt = $this->sql->prepare($query);
            if (!$stmt) {
                throw new MySQLException('Prepare Failed: ' . $this->sql->error);
            }
            if (count($params) > 0) {
                $types = '';
                $bindParams = [];
                foreach ($params as $param) {
                    if (is_int($param)) {
                        $types .= 'i';
                    } elseif (is_float($param)) {
                        $types .= 'd';
                    } elseif (is_null($param)) {
                        // MySQLi converts strings bound as null to SQL NULL.
                        $types .= 's';
                    } elseif (is_resource($param)) {
                        $types .= 'b';
                    } else {
                        $types .= 's';
                    }
                    $bindParams[] = $param;
                }
                if (!$stmt->bind_param($types, ...$bindParams)) {
                    throw new MySQLException('Binding parameters failed: ' . $stmt->error);
                }
            }
            if (!$stmt->execute()) {
                throw new MySQLException('Execute Failed: ' . $stmt->error);
            }
            $result = $stmt->get_result();
            $stmt->close();
            return $result ?: null;
        } catch (\Throwable $e) {
            throw new MySQLException('Prepare and execute failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Executes a callback within a database transaction.
     *
     * @param callable $callback The transactional callback to execute.
     * @return void
     * @throws MySQLException If the transaction fails.
     */
    public function transaction(callable $callback): void
    {
        $this->ping();
        $this->sql->begin_transaction();
        try {
            $callback();
            $this->sql->commit();
        } catch (\Throwable $e) {
            $this->sql->rollback();
            throw new MySQLException('Transaction failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Closes the database connection.
     *
     * @return void
     */
    private function shutdown(): void
    {
        if ($this->sql) {
            try {
                $this->sql->close();
            } catch (\Throwable $e) {
                // Log error or silently ignore.
            }
        }
    }

    /**
     * Destructor.
     */
    public function __destruct()
    {
        $this->shutdown();
    }
}
