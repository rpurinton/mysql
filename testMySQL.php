<?php

require __DIR__ . '/vendor/autoload.php';

use RPurinton\MySQL;

$sql = new MySQL;
$result = $sql->fetch_all("SELECT 1");
if($result[0][1] === "1") echo "Success!\n";
else echo("Failure?!\nExpected 1 but got " . print_r($result, true) . "\n");
