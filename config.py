"""
Environment configuration management for Streamlit app
Supports local, staging, and production environments
"""
import os
import streamlit as st
from typing import Dict, Any


class Config:
    """Base configuration"""
    DEBUG = False
    TESTING = False

    # Default Snowflake settings
    SNOWFLAKE_DATABASE = "upload_db"
    SNOWFLAKE_SCHEMA = "public"

    # Default AWS settings
    AWS_REGION = "us-east-1"
    LAMBDA_FUNCTION_NAME = "register-start-viewership-data-processing"

    # Lambda invocation flag - set to False to disable Lambda calls
    ENABLE_LAMBDA = False  # TODO: Set to True when ready to enable Lambda


class DevelopmentConfig(Config):
    """Development environment configuration"""
    DEBUG = True
    ENVIRONMENT = "development"

    # Can override settings for local dev
    SNOWFLAKE_DATABASE = "upload_db"
    SNOWFLAKE_SCHEMA = "public"
    LAMBDA_FUNCTION_NAME = "register-start-viewership-data-processing-staging"
    ENABLE_LAMBDA = True  # Enabled for testing


class StagingConfig(Config):
    """Staging environment configuration"""
    ENVIRONMENT = "staging"

    # Staging-specific settings
    SNOWFLAKE_DATABASE = "upload_db"
    SNOWFLAKE_SCHEMA = "public"
    LAMBDA_FUNCTION_NAME = "register-start-viewership-data-processing-staging"
    ENABLE_LAMBDA = True  # Enabled for staging testing


class ProductionConfig(Config):
    """Production environment configuration"""
    ENVIRONMENT = "production"

    # Production settings
    SNOWFLAKE_DATABASE = "upload_db_prod"
    SNOWFLAKE_SCHEMA = "public"
    LAMBDA_FUNCTION_NAME = "register-start-viewership-data-processing"
    ENABLE_LAMBDA = True  # Enabled for production


# Configuration mapping
config_map = {
    'development': DevelopmentConfig,
    'staging': StagingConfig,
    'production': ProductionConfig
}


def get_config() -> Config:
    """
    Get configuration based on environment

    Priority:
    1. STREAMLIT_ENV environment variable
    2. secrets.toml [environment] section
    3. Default to development
    """
    # Check environment variable first
    env = os.getenv('STREAMLIT_ENV', '').lower()

    # If not set, check streamlit secrets
    if not env and 'environment' in st.secrets:
        env = st.secrets['environment'].get('name', '').lower()

    # Default to development
    if not env or env not in config_map:
        env = 'development'

    config_class = config_map[env]
    return config_class()


def load_snowflake_config() -> Dict[str, Any]:
    """
    Load Snowflake configuration from secrets and environment config

    Returns:
        Dictionary with Snowflake connection parameters
    """
    config = get_config()
    sf_secrets = st.secrets['snowflake']

    result = {
        'user': sf_secrets['user'],
        'account': sf_secrets['account'],
        'warehouse': sf_secrets['warehouse'],
        'database': config.SNOWFLAKE_DATABASE,
        'schema': config.SNOWFLAKE_SCHEMA,
        'role': 'WEB_APP'
    }

    if 'private_key_b64' in sf_secrets:
        import base64
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend
        pk_pem = base64.b64decode(sf_secrets['private_key_b64'])
        p_key = serialization.load_pem_private_key(pk_pem, password=None, backend=default_backend())
        result['private_key'] = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    else:
        result['password'] = sf_secrets['password']

    return result


def load_aws_config() -> Dict[str, Any]:
    """
    Load AWS configuration from secrets and environment config

    Returns:
        Dictionary with AWS credentials and settings
    """
    config = get_config()

    return {
        'access_key_id': st.secrets['aws']['access_key_id'],
        'secret_access_key': st.secrets['aws']['secret_access_key'],
        'region': config.AWS_REGION,
        'lambda_function_name': config.LAMBDA_FUNCTION_NAME
    }


def get_environment_name() -> str:
    """Get the current environment name"""
    config = get_config()
    return config.ENVIRONMENT
