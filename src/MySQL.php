<?php

declare(strict_types=1);

namespace RPurinton;

use mysqli;
use mysqli_result;
use RPurinton\{Log, Config};
use RPurinton\Validators\MySQLValidators;
use RPurinton\Exceptions\MySQLException;

class MySQL
{
    private ?mysqli $sql = null;
    private int $ping_time = 0;
    private int $wait_timeout = 2700;
    private bool $closed = false;


    /**
     * Constructor.
     *
     * @param array|null $config Optional configuration overrides.
     * @throws MySQLException When initialization fails.
     */
    public function __construct(private ?array $config = null)
    {
        Log::trace(__METHOD__, ['config' => $config]);
        try {
            $this->config = Config::get('MySQL', [
                'host|hostname' => MySQLValidators::validateHost(...),
                'user|username' => MySQLValidators::validateUser(...),
                'password|pass' => MySQLValidators::validatePass(...),
                'database|db'   => MySQLValidators::validateDb(...),
            ], $config);
            Log::trace("Configuration loaded", ['config' => $this->config]);
            $this->reconnect();
            Log::debug("MySQL initialization completed successfully");
        } catch (\Throwable $e) {
            Log::error("Initialization failed", ['error' => $e->getMessage()]);
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
        Log::trace(__METHOD__, ['config' => $config]);
        $instance = new self($config);
        Log::debug("Static connect: connection established");
        return $instance;
    }

    /**
     * Re-establishes a connection to the database.
     *
     * @return void
     * @throws MySQLException On connection failure.
     */
    public function reconnect(): void
    {
        Log::trace(__METHOD__);
        try {
            if ($this->sql) {
                Log::trace("Existing connection detected, closing old connection", ['host' => $this->config['host'] ?? 'unknown']);
                $this->sql->close();
            }
            extract($this->config);
            Log::trace("Attempting database connection", ['host' => $host, 'user' => $user, 'db' => $database]);
            $this->sql = new mysqli($host, $user, $password, $database);
            if ($this->sql->connect_error) {
                Log::error("Connection error", ['errno' => $this->sql->connect_errno, 'error' => $this->sql->connect_error]);
                throw new MySQLException(
                    'Connect Error (' . $this->sql->connect_errno . ') ' . $this->sql->connect_error
                );
            }
            if (!$this->sql->set_charset('utf8mb4')) {
                Log::error("Error setting charset", ['error' => $this->sql->error]);
                throw new MySQLException('Error setting charset: ' . $this->sql->error);
            }
            $this->ping_time = time();
            Log::trace("Ping time set", ['ping_time' => $this->ping_time]);
            $this->wait_timeout = (int)$this->sql->query('SELECT @@wait_timeout')->fetch_row()[0] / 2;
            Log::trace("Wait timeout fetched", ['wait_timeout' => $this->wait_timeout]);
            Log::debug("Database reconnected successfully");
        } catch (\Throwable $e) {
            Log::error("Reconnect failed", ['error' => $e->getMessage()]);
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
        Log::trace(__METHOD__, [
            'ping_time'    => $this->ping_time,
            'wait_timeout' => $this->wait_timeout,
            'current_time' => time(),
        ]);
        if (time() - $this->ping_time < $this->wait_timeout) {
            $this->ping_time = time();
            Log::trace("Ping: within wait_timeout, resetting ping_time", ['new_ping_time' => $this->ping_time]);
            return;
        }
        try {
            Log::trace("Pinging database with query: SELECT 1");
            $this->sql->query('SELECT 1');
            $this->ping_time = time();
            Log::trace("Ping succeeded, updated ping_time", ['new_ping_time' => $this->ping_time]);
        } catch (\Throwable $e) {
            Log::warn("Ping failed, attempting reconnect", ['error' => $e->getMessage()]);
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
        Log::trace(__METHOD__, ['query' => $query]);
        try {
            $this->ping();
            $result = $this->sql->query($query);
            if ($result === false) {
                Log::error("Query error", ['error' => $this->sql->error, 'query' => $query]);
                throw new MySQLException('Query Error: ' . $this->sql->error);
            }
            Log::trace("Query executed", ['result' => $result]);
            return $result === true ? null : $result;
        } catch (\Throwable $e) {
            Log::error("Query failed", ['error' => $e->getMessage(), 'query' => $query]);
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
        Log::trace(__METHOD__, ['query' => $query]);
        try {
            $result = $this->query($query);
            $data = $result->fetch_all(MYSQLI_ASSOC);
            Log::trace("Fetched all rows", ['row_count' => count($data)]);
            return $data;
        } catch (\Throwable $e) {
            Log::error("Fetch all failed", ['error' => $e->getMessage(), 'query' => $query]);
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
        Log::trace(__METHOD__, ['query' => $query]);
        try {
            $result = $this->query($query);
            $row = $result->fetch_assoc();
            Log::trace("Fetched row", ['row' => $row]);
            return $row;
        } catch (\Throwable $e) {
            Log::error("Fetch row failed", ['error' => $e->getMessage(), 'query' => $query]);
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
        Log::trace(__METHOD__, ['query' => $query]);
        try {
            $result = $this->query($query);
            if (!$result) {
                Log::trace("No result set returned", ['query' => $query]);
                return null;
            }
            $row = $result->fetch_row();
            $value = (is_array($row) && isset($row[0])) ? $row[0] : null;
            Log::trace("Fetched single value", ['value' => $value]);
            return $value;
        } catch (\Throwable $e) {
            Log::error("Fetch one failed", ['error' => $e->getMessage(), 'query' => $query]);
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
        Log::trace(__METHOD__, ['query' => $query]);
        try {
            $result = $this->query($query);
            $column = [];
            while ($row = $result->fetch_row()) {
                $column[] = $row[0];
            }
            Log::trace("Fetched column", ['column_count' => count($column)]);
            return $column;
        } catch (\Throwable $e) {
            Log::error("Fetch column failed", ['error' => $e->getMessage(), 'query' => $query]);
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
        Log::trace(__METHOD__, ['query' => $query]);
        try {
            $this->ping();
            if (!$this->sql->multi_query($query)) {
                Log::error("Multi query error", ['error' => $this->sql->error]);
                throw new MySQLException('Multi Query Error: ' . $this->sql->error);
            }
            Log::trace("Multi query executed");
            $results = [];
            do {
                $result = $this->sql->store_result();
                if ($result) {
                    $data = $result->fetch_all(MYSQLI_ASSOC);
                    $results[] = $data;
                    Log::trace("Multi query step", ['rows' => count($data)]);
                    $result->free();
                }
            } while ($this->sql->more_results() && $this->sql->next_result());
            Log::debug("Multi query completed", ['result_sets' => count($results)]);
            return $results;
        } catch (\Throwable $e) {
            Log::error("Multi query failed", ['error' => $e->getMessage()]);
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
        Log::trace(__METHOD__, ['query' => $query]);
        try {
            $this->query($query);
            $id = $this->sql->insert_id;
            Log::trace("Insert successful", ['insert_id' => $id]);
            return $id;
        } catch (\Throwable $e) {
            Log::error("Insert failed", ['error' => $e->getMessage(), 'query' => $query]);
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
    public function escape(string|array|null $input): string|array|null
    {
        Log::trace(__METHOD__, ['input' => $input]);
        try {
            if ($input === null) {
                Log::trace("Input is null");
                return null;
            }
            if (is_array($input)) {
                $escaped = array_map([$this, 'escape'], $input);
                Log::trace("Escaped array", ['escaped' => $escaped]);
                return $escaped;
            }
            $escaped = $this->sql->real_escape_string($input);
            Log::trace("Escaped string", ['escaped' => $escaped]);
            return $escaped;
        } catch (\Throwable $e) {
            Log::error("Escape failed", ['error' => $e->getMessage(), 'input' => $input]);
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
        Log::trace(__METHOD__, ['insert_id' => $this->sql->insert_id]);
        return $this->sql->insert_id;
    }

    /**
     * Returns the number of rows affected by the last query.
     *
     * @return int
     */
    public function affected_rows(): int
    {
        Log::trace(__METHOD__, ['affected_rows' => $this->sql->affected_rows]);
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
        Log::trace(__METHOD__, ['query' => $query, 'params' => $params]);
        try {
            $this->ping();
            $stmt = $this->sql->prepare($query);
            if (!$stmt) {
                Log::error("Prepare failed", ['error' => $this->sql->error, 'query' => $query]);
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
                        $types .= 's';
                    } elseif (is_resource($param)) {
                        $types .= 'b';
                    } else {
                        $types .= 's';
                    }
                    $bindParams[] = $param;
                }
                Log::trace("Binding parameters", ['types' => $types, 'params' => $bindParams]);
                if (!$stmt->bind_param($types, ...$bindParams)) {
                    Log::error("Binding parameters failed", ['error' => $stmt->error]);
                    throw new MySQLException('Binding parameters failed: ' . $stmt->error);
                }
            }
            if (!$stmt->execute()) {
                Log::error("Execute failed", ['error' => $stmt->error]);
                throw new MySQLException('Execute Failed: ' . $stmt->error);
            }
            $result = $stmt->get_result();
            $stmt->close();
            Log::debug("Prepare and execute completed", ['result' => $result !== null ? 'result set returned' : 'no result set']);
            return $result ?: null;
        } catch (\Throwable $e) {
            Log::error("Prepare and execute failed", ['error' => $e->getMessage(), 'query' => $query]);
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
        Log::trace(__METHOD__);
        $this->ping();
        $this->sql->begin_transaction();
        try {
            $callback();
            $this->sql->commit();
            Log::debug("Transaction committed successfully");
        } catch (\Throwable $e) {
            $this->sql->rollback();
            Log::warn("Transaction rolled back", ['error' => $e->getMessage()]);
            throw new MySQLException('Transaction failed: ' . $e->getMessage(), (int)$e->getCode(), $e);
        }
    }

    /**
     * Closes the database connection.
     *
     * @return void
     */
    public function shutdown(): void
    {
        Log::trace(__METHOD__);
        if ($this->closed || !$this->sql) {
            Log::trace("Connection already closed or not initialized");
            return;
        }
        try {
            $this->sql->close();
            $this->closed = true;
            Log::debug("Database connection closed");
        } catch (\Throwable $e) {
            Log::error("Error during shutdown", ['error' => $e->getMessage()]);
        }
    }

    /**
     * Destructor.
     */
    public function __destruct()
    {
        Log::trace(__METHOD__);
        $this->shutdown();
    }
}
