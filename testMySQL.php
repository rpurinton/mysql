#!/usr/bin/env php
<?php

require __DIR__ . '/vendor/autoload.php';

use RPurinton\{Log, MySQL};

Log::install();
$sql = MySQL::connect();
$query = "
    SELECT data
    FROM users
    WHERE id = 'e7ac6ea8-e6a9-11ef-9de4-03dd4eedb9d9'";
$result = json_decode($sql->fetch_one($query), true);
if($result['display_name'] === "Russell") die("Hello Russell!\n");
echo("Failure?!\nExpected 'Russell' but got " . print_r($result, true) . "\n");
