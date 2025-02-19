<?php

declare(strict_types=1);

namespace RPurinton;

use mysqli;
use mysqli_result;
use RPurinton\Config;
use RPurinton\Exceptions\MySQLException;

class MySQL
{
    private mysqli $sql;

    public function __construct(array $config = null)
    {
        if (!$config) $config = Config::get("MySQL", [
            "host" => "string",
            "user" => "string",
            "pass" => "string",
            "db" => "string",
        ]);

        $this->sql = new mysqli($config['host'], $config['user'], $config['pass'], $config['db']);
        if ($this->sql->connect_error) {
            throw new MySQLException('Connect Error (' . $this->sql->connect_errno . ') ' . $this->sql->connect_error);
        }
        if (!$this->sql->set_charset('utf8mb4')) {
            throw new MySQLException('Error setting charset: ' . $this->sql->error);
        }
    }

    public static function connect($config = null): MySQL
    {
        return new self($config);
    }

    public function __destruct()
    {
        if (isset($this->sql)) {
            $this->sql->close();
        }
    }

    /**
     * Executes a query and returns a mysqli_result object, or null for successful non-result queries.
     *
     * @param string $query
     * @return mysqli_result|null
     */
    public function query(string $query): ?mysqli_result
    {
        $result = $this->sql->query($query);
        if ($result === false) {
            throw new MySQLException('Query Error: ' . $this->sql->error);
        }
        return $result === true ? null : $result;
    }

    /**
     * Executes a SELECT query and returns all rows as an associative array.
     *
     * @param string $query
     * @return array
     */
    public function fetch_all(string $query): array
    {
        $result = $this->query($query);
        return $result->fetch_all(MYSQLI_ASSOC);
    }

    /**
     * Executes a SELECT query and returns the first row as an associative array.
     *
     * @param string $query
     * @return array
     */
    public function fetch_row(string $query): array
    {
        $result = $this->query($query);
        return $result->fetch_assoc();
    }

    /**
     * Executes a SELECT query and returns the first column of the first row.
     *
     * @param string $query
     * @return mixed
     */
    public function fetch_one(string $query): mixed
    {
        $result = $this->query($query);
        $row = $result->fetch_row();
        return $row[0];
    }

    /**
     * Executes a SELECT query and returns the first column of all rows.
     *
     * @param string $query
     * @return array
     */
    public function fetch_column(string $query): array
    {
        $result = $this->query($query);
        $column = [];
        while ($row = $result->fetch_row()) {
            $column[] = $row[0];
        }
        return $column;
    }

    /**
     * Execute multiple queries in a single call and return an array of results.
     * 
     * @param string $query
     * @return array
     */
    public function multi(string $query): array
    {
        $this->sql->multi_query($query);
        $results = [];
        do {
            $result = $this->sql->store_result();
            if ($result) {
                $results[] = $result->fetch_all(MYSQLI_ASSOC);
                $result->free();
            }
        } while ($this->sql->more_results() && $this->sql->next_result());
        return $results;
    }

    /**
     * Executes an INSERT statement and returns the ID of the new row.
     * 
     * @param string $query
     * @return int|string
     */
    public function insert(string $query)
    {
        $this->query($query);
        return $this->sql->insert_id;
    }

    /**
     * Escapes a string or array of strings for use in a query.
     *
     * @param string|array $input
     * @return string|array
     */
    public function escape(string|array $input): string|array
    {
        if (is_array($input)) {
            return array_map([$this, 'escape'], $input);
        }
        return $this->sql->real_escape_string($input);
    }

    /**
     * Returns the ID generated by the most recent INSERT query.
     *
     * @return int
     */
    public function last_insert_id(): int
    {
        return (int) $this->sql->insert_id;
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
     * Prepares and executes a query with parameters.
     *
     * @param string $query
     * @param array $params
     * @return mysqli_result|null
     */
    public function prepareAndExecute(string $query, array $params = []): ?mysqli_result
    {
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
                    // Bind nulls as strings; MySQLi converts them to SQL NULL.
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
    }

    /**
     * Executes a transactional callback.
     *
     * @param callable $callback
     * @return void
     */
    public function transaction(callable $callback): void
    {
        $this->sql->begin_transaction();
        try {
            $callback();
            $this->sql->commit();
        } catch (\Throwable $e) {
            $this->sql->rollback();
            throw $e;
        }
    }
}
