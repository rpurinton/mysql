<?php

namespace RPurinton\Validators;

class MySQLValidators
{
    public static function validateHost(string $host): bool
    {
        return filter_var($host, FILTER_VALIDATE_IP) || filter_var($host, FILTER_VALIDATE_DOMAIN);
    }
    public static function validateUser(string $user): bool
    {
        return strlen($user) > 0;
    }
    public static function validatePass(string $pass): bool
    {
        return strlen($pass) > 0;
    }
    public static function validateDb(string $db): bool
    {
        return strlen($db) > 0;
    }
}
