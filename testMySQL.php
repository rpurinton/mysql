#!/usr/bin/env php
<?php

require __DIR__ . '/vendor/autoload.php';

use RPurinton\{Log, MySQL};

Log::install();
$sql = MySQL::connect();
$query = "
    SELECT data
    FROM users
    WHERE id = '12345'";
$result = json_decode($sql->fetch_one($query), true);
if($result['username'] === "foo") die("Test OK\n");
echo("Failure?!\nExpected username 'foo'  but got " . print_r($result, true) . "\n");
