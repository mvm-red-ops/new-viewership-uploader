import snowflake.connector
import streamlit as st
import json
from typing import Dict, List, Optional
import uuid
from datetime import datetime
import pandas as pd
from config import load_snowflake_config

class SnowflakeConnection:
    """Handle Snowflake database operations"""

    def __init__(self):
        """Initialize Snowflake connection using environment-aware config"""
        try:
            sf_config = load_snowflake_config()
            self.conn = snowflake.connector.connect(**sf_config)
            self.cursor = self.conn.cursor()
            self.database = sf_config['database']  # Store database name
            self.schema = sf_config['schema']  # Store schema name

            # Explicitly set the database and schema context
            self.cursor.execute(f"USE DATABASE {self.database}")
            self.cursor.execute(f"USE SCHEMA {self.schema}")

            # Verify the context is set correctly
            self.cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
            current_db, current_schema = self.cursor.fetchone()
            print(f"[DEBUG] Connected to Snowflake - Database: {current_db}, Schema: {current_schema}")

            if current_db != self.database.upper():
                raise Exception(f"Database context mismatch! Expected {self.database}, got {current_db}")

            self._ensure_table_exists()
        except KeyError as e:
            raise Exception(f"Missing Snowflake configuration: {str(e)}")
        except Exception as e:
            raise Exception(f"Failed to connect to Snowflake: {str(e)}")

    def is_connected(self) -> bool:
        """
        Check if the connection is still alive and valid.

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            if self.conn is None or self.cursor is None:
                return False
            # Simple query to check if connection is alive
            self.cursor.execute("SELECT 1")
            self.cursor.fetchone()
            return True
        except Exception:
            return False

    def _ensure_table_exists(self):
        """Create the viewership_file_formats table if it doesn't exist"""
        # File formats table lives in the configured database
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS dictionary.public.viewership_file_formats (
            config_id VARCHAR(36) PRIMARY KEY,
            platform VARCHAR(255) NOT NULL,
            partner VARCHAR(255) NOT NULL,
            channel VARCHAR(255),
            territory VARCHAR(255),
            territories ARRAY,
            domain VARCHAR(255),
            column_mappings VARIANT NOT NULL,
            validation_rules VARIANT,
            filename_pattern VARCHAR(500),
            source_columns VARIANT,
            target_table VARCHAR(255),
            sample_data VARIANT,
            created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            created_by VARCHAR(255),
            custom_sanitization_procedure VARCHAR(100),
            custom_territory_procedure VARCHAR(100),
            custom_channel_procedure VARCHAR(100),
            custom_date_procedure VARCHAR(100),
            custom_normalizers VARIANT,
            CONSTRAINT unique_template_config UNIQUE (platform, partner, channel)
        )
        """
        try:
            self.cursor.execute(create_table_sql)
            self.conn.commit()
        except Exception as e:
            # Table might already exist
            pass

    def insert_config(self, config_data: Dict) -> str:
        """
        Insert a new configuration into the database

        Args:
            config_data: Dictionary containing configuration details

        Returns:
            config_id: The unique ID of the inserted configuration
        """
        config_id = str(uuid.uuid4())

        insert_sql = f"""
        INSERT INTO dictionary.public.viewership_file_formats (
            config_id,
            platform,
            partner,
            channel,
            territory,
            territories,
            domain,
            column_mappings,
            validation_rules,
            filename_pattern,
            source_columns,
            target_table,
            sample_data,
            created_date,
            updated_date,
            custom_sanitization_procedure,
            custom_territory_procedure,
            custom_channel_procedure,
            custom_date_procedure,
            custom_normalizers
        )
        SELECT %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, PARSE_JSON(%s), PARSE_JSON(%s), %s, PARSE_JSON(%s), %s, PARSE_JSON(%s), %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
        """

        values = (
            config_id,
            config_data.get('platform'),
            config_data.get('partner'),
            config_data.get('channel'),  # NULL is allowed for platform-wide configs
            config_data.get('territory'),  # Keep for backward compatibility
            json.dumps(config_data.get('territories', [])),  # NEW: Array of territories
            config_data.get('domain'),
            json.dumps(config_data.get('column_mappings', {})),
            json.dumps(config_data.get('validation_rules', {})),
            config_data.get('filename_pattern'),
            json.dumps(config_data.get('source_columns', [])),
            config_data.get('target_table'),
            json.dumps(config_data.get('sample_data', [])),
            datetime.now(),
            datetime.now(),
            config_data.get('custom_sanitization_procedure'),
            config_data.get('custom_territory_procedure'),
            config_data.get('custom_channel_procedure'),
            config_data.get('custom_date_procedure'),
            json.dumps(config_data.get('custom_normalizers', {}))
        )

        try:
            print(f"[DEBUG] Attempting INSERT with values:")
            print(f"  Platform: {config_data.get('platform')}")
            print(f"  Partner: {config_data.get('partner')}")
            print(f"  Channel: '{config_data.get('channel', '')}'")
            print(f"  Territory: '{config_data.get('territory', '')}'")
            print(f"[DEBUG] INSERT SQL: {insert_sql}")

            self.cursor.execute(insert_sql, values)
            self.conn.commit()
            print(f"[DEBUG] INSERT successful! Config ID: {config_id}")
            return config_id
        except snowflake.connector.errors.IntegrityError as e:
            print(f"[DEBUG] IntegrityError caught: {str(e)}")
            print(f"[DEBUG] Checking what's in the table...")
            self.cursor.execute("SELECT config_id, platform, partner, channel, territory FROM dictionary.public.viewership_file_formats")
            existing = self.cursor.fetchall()
            print(f"[DEBUG] Existing rows in table: {existing}")
            raise Exception(f"Configuration already exists for Platform: {config_data.get('platform')}, Partner: {config_data.get('partner')}")
        except Exception as e:
            print(f"[DEBUG] Other error: {str(e)}")
            self.conn.rollback()
            raise Exception(f"Error inserting configuration: {str(e)}")

    def update_config(self, config_id: str, config_data: Dict):
        """
        Update an existing configuration

        Args:
            config_id: The unique ID of the configuration to update
            config_data: Dictionary containing updated configuration details
        """
        update_sql = f"""
        UPDATE dictionary.public.viewership_file_formats
        SET
            platform = %s,
            partner = %s,
            channel = %s,
            territory = %s,
            territories = PARSE_JSON(%s),
            domain = %s,
            column_mappings = PARSE_JSON(%s),
            validation_rules = PARSE_JSON(%s),
            filename_pattern = %s,
            source_columns = PARSE_JSON(%s),
            target_table = %s,
            sample_data = PARSE_JSON(%s),
            updated_date = %s,
            custom_sanitization_procedure = %s,
            custom_territory_procedure = %s,
            custom_channel_procedure = %s,
            custom_date_procedure = %s,
            custom_normalizers = PARSE_JSON(%s)
        WHERE config_id = %s
        """

        values = (
            config_data.get('platform'),
            config_data.get('partner'),
            config_data.get('channel'),  # NULL is allowed for platform-wide configs
            config_data.get('territory'),  # Keep for backward compatibility
            json.dumps(config_data.get('territories', [])),  # NEW: Array of territories
            config_data.get('domain'),
            json.dumps(config_data.get('column_mappings', {})),
            json.dumps(config_data.get('validation_rules', {})),
            config_data.get('filename_pattern'),
            json.dumps(config_data.get('source_columns', [])),
            config_data.get('target_table'),
            json.dumps(config_data.get('sample_data', [])),
            datetime.now(),
            config_data.get('custom_sanitization_procedure'),
            config_data.get('custom_territory_procedure'),
            config_data.get('custom_channel_procedure'),
            config_data.get('custom_date_procedure'),
            json.dumps(config_data.get('custom_normalizers', {})),
            config_id
        )

        try:
            self.cursor.execute(update_sql, values)
            self.conn.commit()

            if self.cursor.rowcount == 0:
                raise Exception(f"No configuration found with ID: {config_id}")
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error updating configuration: {str(e)}")

    def delete_config(self, config_id: str):
        """
        Delete a configuration

        Args:
            config_id: The unique ID of the configuration to delete
        """
        delete_sql = f"DELETE FROM dictionary.public.viewership_file_formats WHERE config_id = %s"

        try:
            self.cursor.execute(delete_sql, (config_id,))
            self.conn.commit()

            if self.cursor.rowcount == 0:
                raise Exception(f"No configuration found with ID: {config_id}")
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error deleting configuration: {str(e)}")

    def get_config_by_id(self, config_id: str) -> Optional[Dict]:
        """
        Retrieve a configuration by its ID

        Args:
            config_id: The unique ID of the configuration

        Returns:
            Dictionary containing configuration details or None if not found
        """
        select_sql = f"""
        SELECT
            config_id,
            platform,
            partner,
            channel,
            territory,
            column_mappings,
            validation_rules,
            filename_pattern,
            source_columns,
            target_table,
            created_date,
            updated_date,
            created_by,
            custom_sanitization_procedure,
            custom_territory_procedure,
            custom_channel_procedure,
            custom_date_procedure,
            custom_normalizers,
            domain,
            sample_data,
            data_type,
            territories
        FROM dictionary.public.viewership_file_formats
        WHERE config_id = %s
        """

        try:
            self.cursor.execute(select_sql, (config_id,))
            row = self.cursor.fetchone()

            if row:
                return self._row_to_dict(row)
            return None
        except Exception as e:
            raise Exception(f"Error retrieving configuration: {str(e)}")

    def get_config_by_platform_partner(self, platform: str, partner: str) -> Optional[Dict]:
        """
        Retrieve a configuration by platform and partner

        Args:
            platform: The platform name
            partner: The partner name

        Returns:
            Dictionary containing configuration details or None if not found
        """
        select_sql = f"""
        SELECT
            config_id,
            platform,
            partner,
            channel,
            territory,
            column_mappings,
            validation_rules,
            filename_pattern,
            source_columns,
            target_table,
            created_date,
            updated_date,
            created_by,
            custom_sanitization_procedure,
            custom_territory_procedure,
            custom_channel_procedure,
            custom_date_procedure,
            custom_normalizers,
            domain,
            sample_data,
            data_type,
            territories
        FROM dictionary.public.viewership_file_formats
        WHERE LOWER(platform) = LOWER(%s) AND LOWER(partner) = LOWER(%s)
        """

        try:
            self.cursor.execute(select_sql, (platform, partner))
            row = self.cursor.fetchone()

            if row:
                return self._row_to_dict(row)
            return None
        except Exception as e:
            raise Exception(f"Error retrieving configuration: {str(e)}")

    def get_platforms(self) -> List[str]:
        """
        Retrieve list of platforms from dictionary.public.platforms

        Returns:
            List of platform names
        """
        select_sql = """
        SELECT name
        FROM dictionary.public.platforms
        GROUP BY ALL
        ORDER BY name ASC
        """

        try:
            self.cursor.execute(select_sql)
            rows = self.cursor.fetchall()
            return [row[0] for row in rows if row[0]]
        except Exception as e:
            # If table doesn't exist or query fails, return empty list
            print(f"Warning: Could not fetch platforms from dictionary.public.platforms: {str(e)}")
            return []

    def get_channels(self) -> List[str]:
        """
        Retrieve list of channels from dictionary.public.channels

        Returns:
            List of channel names
        """
        select_sql = """
        SELECT name
        FROM dictionary.public.channels
        GROUP BY ALL
        ORDER BY name ASC
        """

        try:
            self.cursor.execute(select_sql)
            rows = self.cursor.fetchall()
            return [row[0] for row in rows if row[0]]
        except Exception as e:
            print(f"Warning: Could not fetch channels from dictionary.public.channels: {str(e)}")
            return []

    def get_territories(self) -> List[str]:
        """
        Retrieve list of territories from dictionary.public.territories

        Returns:
            List of territory names
        """
        select_sql = """
        SELECT name
        FROM dictionary.public.territories
        GROUP BY ALL
        ORDER BY name ASC
        """

        try:
            self.cursor.execute(select_sql)
            rows = self.cursor.fetchall()
            return [row[0] for row in rows if row[0]]
        except Exception as e:
            print(f"Warning: Could not fetch territories from dictionary.public.territories: {str(e)}")
            return []

    def get_partners(self) -> List[str]:
        """
        Retrieve list of partners from dictionary.public.partners

        Returns:
            List of partner names
        """
        select_sql = """
        SELECT name
        FROM dictionary.public.partners
        WHERE active = true
        ORDER BY name ASC
        """

        try:
            self.cursor.execute(select_sql)
            rows = self.cursor.fetchall()
            return [row[0] for row in rows if row[0]]
        except Exception as e:
            print(f"Warning: Could not fetch partners from dictionary.public.partners: {str(e)}")
            return []

    def check_duplicate_config(self, platform: str, partner: str, channel: Optional[str] = None) -> Optional[Dict]:
        """
        Check if exact configuration already exists (matches UNIQUE constraint)

        NOTE: Territory is not checked - new constraint allows same platform/partner/channel
        with different territories arrays.

        Args:
            platform: Platform name (exact match)
            partner: Partner name (exact match)
            channel: Channel name (exact match, NULL if None)

        Returns:
            Dictionary with config details if duplicate exists, None otherwise
        """
        # Handle NULL comparison properly in SQL
        if channel:
            channel_clause = "channel = %s"
            params = [platform, partner, channel]
        else:
            channel_clause = "channel IS NULL"
            params = [platform, partner]

        select_sql = f"""
        SELECT
            config_id,
            platform,
            partner,
            channel,
            territory,
            column_mappings,
            validation_rules,
            filename_pattern,
            source_columns,
            target_table,
            created_date,
            updated_date,
            created_by,
            custom_sanitization_procedure,
            custom_territory_procedure,
            custom_channel_procedure,
            custom_date_procedure,
            custom_normalizers,
            domain,
            sample_data,
            data_type,
            territories
        FROM dictionary.public.viewership_file_formats
        WHERE platform = %s
          AND partner = %s
          AND {channel_clause}
        """

        try:
            self.cursor.execute(select_sql, params)
            row = self.cursor.fetchone()
            return self._row_to_dict(row) if row else None
        except Exception as e:
            raise Exception(f"Error checking for duplicate config: {str(e)}")

    def search_configs(self, platform: Optional[str] = None, partner: Optional[str] = None) -> List[Dict]:
        """
        Search for configurations by platform and/or partner

        Args:
            platform: Optional platform name (partial match)
            partner: Optional partner name (partial match)

        Returns:
            List of dictionaries containing configuration details
        """
        conditions = []
        params = []

        if platform:
            conditions.append("LOWER(platform) LIKE LOWER(%s)")
            params.append(f"%{platform}%")

        if partner:
            conditions.append("LOWER(partner) LIKE LOWER(%s)")
            params.append(f"%{partner}%")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        select_sql = f"""
        SELECT
            config_id,
            platform,
            partner,
            channel,
            territory,
            column_mappings,
            validation_rules,
            filename_pattern,
            source_columns,
            target_table,
            created_date,
            updated_date,
            created_by,
            custom_sanitization_procedure,
            custom_territory_procedure,
            custom_channel_procedure,
            custom_date_procedure,
            custom_normalizers,
            domain,
            sample_data,
            data_type,
            territories
        FROM dictionary.public.viewership_file_formats
        WHERE {where_clause}
        ORDER BY platform, partner
        """

        try:
            self.cursor.execute(select_sql, params)
            rows = self.cursor.fetchall()
            return [self._row_to_dict(row) for row in rows]
        except Exception as e:
            raise Exception(f"Error searching configurations: {str(e)}")

    def get_platforms(self) -> List[str]:
        """
        Retrieve list of platforms from dictionary.public.platforms

        Returns:
            List of platform names
        """
        select_sql = """
        SELECT name
        FROM dictionary.public.platforms
        GROUP BY ALL
        ORDER BY name ASC
        """

        try:
            self.cursor.execute(select_sql)
            rows = self.cursor.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            # If table doesn't exist or query fails, return empty list
            print(f"Error fetching platforms: {str(e)}")
            return []

    def get_all_configs(self) -> List[Dict]:
        """
        Retrieve all configurations

        Returns:
            List of dictionaries containing configuration details
        """
        select_sql = f"""
        SELECT
            config_id,
            platform,
            partner,
            channel,
            territory,
            column_mappings,
            validation_rules,
            filename_pattern,
            source_columns,
            target_table,
            created_date,
            updated_date,
            created_by,
            custom_sanitization_procedure,
            custom_territory_procedure,
            custom_channel_procedure,
            custom_date_procedure,
            custom_normalizers,
            domain,
            sample_data,
            data_type,
            territories
        FROM dictionary.public.viewership_file_formats
        ORDER BY platform, partner
        """

        try:
            self.cursor.execute(select_sql)
            rows = self.cursor.fetchall()
            return [self._row_to_dict(row) for row in rows]
        except Exception as e:
            raise Exception(f"Error retrieving all configurations: {str(e)}")

    def _row_to_dict(self, row) -> Dict:
        """Convert a database row to a dictionary"""
        return {
            'CONFIG_ID': row[0],
            'PLATFORM': row[1],
            'PARTNER': row[2],
            'CHANNEL': row[3],
            'TERRITORY': row[4],  # Keep for backward compatibility
            'COLUMN_MAPPINGS': json.loads(row[5]) if isinstance(row[5], str) else row[5],
            'VALIDATION_RULES': json.loads(row[6]) if row[6] and isinstance(row[6], str) else (row[6] or {}),
            'FILENAME_PATTERN': row[7],
            'SOURCE_COLUMNS': json.loads(row[8]) if row[8] and isinstance(row[8], str) else (row[8] or []),
            'TARGET_TABLE': row[9],
            'CREATED_DATE': row[10],
            'UPDATED_DATE': row[11],
            'CREATED_BY': row[12],
            'CUSTOM_SANITIZATION_PROCEDURE': row[13],
            'CUSTOM_TERRITORY_PROCEDURE': row[14],
            'CUSTOM_CHANNEL_PROCEDURE': row[15],
            'CUSTOM_DATE_PROCEDURE': row[16],
            'CUSTOM_NORMALIZERS': json.loads(row[17]) if row[17] and isinstance(row[17], str) else (row[17] or {}),
            'DOMAIN': row[18],
            'SAMPLE_DATA': json.loads(row[19]) if row[19] and isinstance(row[19], str) else (row[19] or []),
            'DATA_TYPE': row[20],
            'TERRITORIES': json.loads(row[21]) if row[21] and isinstance(row[21], str) else (row[21] or [])
        }

    def load_to_platform_viewership(self, df, progress_callback=None) -> int:
        """
        Load data into the platform_viewership table

        Args:
            df: Pandas DataFrame with transformed data
            progress_callback: Optional callback function(batch_num, total_batches, rows_in_batch)

        Returns:
            Number of rows inserted
        """
        if df.empty:
            raise Exception("No data to load")

        try:
            # Get column names from dataframe
            columns = df.columns.tolist()

            # Create placeholder string for SQL
            placeholders = ', '.join(['%s'] * len(columns))
            column_names = ', '.join(columns)

            # Create INSERT statement with full database path
            full_table_name = f"{self.database}.{self.schema}.platform_viewership"
            print(f"[DEBUG] Inserting into table: {full_table_name}")
            insert_sql = f"""
            INSERT INTO {full_table_name} ({column_names})
            VALUES ({placeholders})
            """

            # Convert dataframe to list of tuples
            rows = [tuple(row) for row in df.values]
            print(f"[DEBUG] Total rows to insert: {len(rows)}")

            # Batch insert in chunks to avoid Snowflake expression limit
            # Snowflake limit is 200k expressions. With ~12 columns, we can do ~16k rows per batch safely
            batch_size = 16000
            total_inserted = 0
            total_batches = (len(rows) + batch_size - 1) // batch_size

            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]

                # Build VALUES clause for batch
                values_list = []
                for row in batch:
                    # Convert each value to string representation for SQL
                    formatted_values = []
                    for idx, val in enumerate(row):
                        col_name = columns[idx]

                        if val is None or (isinstance(val, float) and pd.isna(val)):
                            formatted_values.append('NULL')
                        elif col_name == 'DATE':
                            # Handle date column specially - convert to YYYY-MM-DD format
                            if isinstance(val, str):
                                try:
                                    # Try to parse the date and convert to YYYY-MM-DD
                                    parsed_date = pd.to_datetime(val)
                                    formatted_values.append(f"'{parsed_date.strftime('%Y-%m-%d')}'")
                                except:
                                    # If parsing fails, pass as-is and let Snowflake handle it
                                    formatted_values.append(f"'{val}'")
                            elif isinstance(val, pd.Timestamp):
                                formatted_values.append(f"'{val.strftime('%Y-%m-%d')}'")
                            else:
                                formatted_values.append(f"'{str(val)}'")
                        elif col_name in ['END_TIME', 'START_TIME', 'LOAD_TIMESTAMP']:
                            # Handle timestamp columns - convert to YYYY-MM-DD HH:MM:SS format
                            if isinstance(val, str):
                                try:
                                    # Try to parse the timestamp with dayfirst=True for DD-MM-YYYY formats
                                    parsed_ts = pd.to_datetime(val, dayfirst=True)
                                    formatted_values.append(f"'{parsed_ts.strftime('%Y-%m-%d %H:%M:%S')}'")
                                except:
                                    # If parsing fails, try without dayfirst
                                    try:
                                        parsed_ts = pd.to_datetime(val)
                                        formatted_values.append(f"'{parsed_ts.strftime('%Y-%m-%d %H:%M:%S')}'")
                                    except:
                                        # Last resort: pass as-is and let Snowflake handle it
                                        formatted_values.append(f"'{val}'")
                            elif isinstance(val, pd.Timestamp):
                                formatted_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                            else:
                                formatted_values.append(f"'{str(val)}'")
                        elif col_name == 'REVENUE':
                            # Handle revenue - strip currency formatting ($, commas, spaces)
                            if isinstance(val, str):
                                # Remove $, spaces, and commas
                                cleaned_val = val.replace('$', '').replace(',', '').replace(' ', '').strip()
                                # Handle special cases like "-" or empty
                                if cleaned_val == '-' or cleaned_val == '':
                                    formatted_values.append('NULL')
                                else:
                                    try:
                                        # Try to convert to float to validate
                                        float_val = float(cleaned_val)
                                        formatted_values.append(str(float_val))
                                    except:
                                        # If still not a number, set to NULL
                                        formatted_values.append('NULL')
                            else:
                                # Already numeric
                                formatted_values.append(str(val))
                        elif isinstance(val, str):
                            # Escape single quotes
                            escaped_val = val.replace("'", "''")
                            formatted_values.append(f"'{escaped_val}'")
                        else:
                            formatted_values.append(str(val))
                    values_list.append(f"({', '.join(formatted_values)})")

                # Create multi-row INSERT statement with full database path
                batch_insert_sql = f"""
                INSERT INTO {full_table_name} ({column_names})
                VALUES {', '.join(values_list)}
                """

                self.cursor.execute(batch_insert_sql)
                self.conn.commit()
                total_inserted += len(batch)

                print(f"[DEBUG] Batch {(i // batch_size) + 1}/{total_batches} committed: {len(batch)} rows into {full_table_name}")

                # Call progress callback if provided
                if progress_callback:
                    batch_num = (i // batch_size) + 1
                    progress_callback(batch_num, total_batches, len(batch))

            # Verify data was inserted
            verify_sql = f"SELECT COUNT(*) FROM {full_table_name}"
            self.cursor.execute(verify_sql)
            total_count = self.cursor.fetchone()[0]
            print(f"[DEBUG] Verification - Total rows in {full_table_name}: {total_count}")

            return total_inserted

        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Error loading data to platform_viewership: {str(e)}")

    def close(self):
        """Close the database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
