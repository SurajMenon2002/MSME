import os
import zipfile
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def extract_zip(zip_path, extract_to):
    """Extracts all files from a zip archive to a given directory."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def get_database_connection(db_name, user, password, host, port):
    """Creates a connection to the PostgreSQL database."""
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
    return engine

def clean_column_names(columns):
    """Cleans column names to be SQL-friendly by replacing spaces, hyphens, and special characters."""
    return [col.strip().replace(' ', '_').replace('-', '_').replace('.', '_').lower() for col in columns]

def truncate_table_name(name, max_length=63):
    """Truncates table name to fit PostgreSQL's 63-character limit while ensuring uniqueness."""
    if len(name) > max_length:
        return name[:max_length - 4] + "_tbl"  # Adding "_tbl" to avoid duplicate truncations
    return name

def upload_excel_to_postgres(folder_path, engine):
    """Reads all Excel files in a folder and uploads them as tables in PostgreSQL."""
    for file in os.listdir(folder_path):
        if file.endswith(".xlsx") or file.endswith(".xls"):
            file_path = os.path.join(folder_path, file)
            xls = pd.ExcelFile(file_path)
            
            for sheet_name in xls.sheet_names:
                df = xls.parse(sheet_name)

                if df.empty:
                    continue  # Skip empty sheets
                
                df.columns = clean_column_names(df.columns.astype(str))
                table_name = f"{os.path.splitext(file)[0].lower()}_{sheet_name.lower()}"
                table_name = table_name.replace(' ', '_').replace('-', '_')
                table_name = truncate_table_name(table_name)

                try:
                    df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=1000)
                    print(f"Uploaded {file} - {sheet_name} to table {table_name}")
                except Exception as e:
                    print(f"Error uploading {file} - {sheet_name} to table {table_name}: {e}")


if __name__ == "__main__":
    # Define paths and DB credentials
    zip_path = "A2. MSMEs to Total (%).zip"  # Replace with actual path
    extract_to = "extracted_files1"
    os.makedirs(extract_to, exist_ok=True)
    
    # Database credentials (update with real values)
    db_config = {
        "db_name": "final",
        "user": "postgres",
        "password": "admin",
        "host": "localhost",
        "port": "5432"
    }
    
    # Extract ZIP and upload files
    extract_zip(zip_path, extract_to)
    engine = get_database_connection(**db_config)
    upload_excel_to_postgres(extract_to, engine)
