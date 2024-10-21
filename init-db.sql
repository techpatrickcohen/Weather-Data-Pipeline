DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database
      WHERE datname = 'weather_db'
   ) THEN
      PERFORM dblink_exec('dbname=' || current_database(), 'CREATE DATABASE weather_db');
   END IF;
END
$do$;