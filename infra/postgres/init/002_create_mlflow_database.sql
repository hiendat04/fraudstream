SELECT 'CREATE DATABASE mlflow OWNER ' || quote_ident(current_user)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'mlflow')\gexec
