import psycopg2

def list_postgres_tables(db_name, user, password, host, port, schema='public'):
    """Lists all table names in the specified PostgreSQL schema."""
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE';
        """
        cursor.execute(query)
        tables = cursor.fetchall()

        print(f"Tables in schema '{schema}':")
        for table in tables:
            print(f"- {table[0]}")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Database credentials (update if needed)
    db_config = {
        "db_name": "final",
        "user": "postgres",
        "password": "admin",
        "host": "localhost",
        "port": "5432"
    }

    list_postgres_tables(**db_config)
