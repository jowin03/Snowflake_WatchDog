from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    snowflake_account: str = Field(..., env="SNOWFLAKE_ACCOUNT")
    snowflake_user: str = Field(..., env="SNOWFLAKE_USER")
    snowflake_password: str = Field(..., env="SNOWFLAKE_PASSWORD")
    snowflake_database: str = Field(..., env="SNOWFLAKE_DATABASE")
    snowflake_warehouse: str = Field(..., env="SNOWFLAKE_WAREHOUSE")
    snowflake_role: str = Field(..., env="SNOWFLAKE_ROLE")
    snowflake_schema: str = Field(..., env="SNOWFLAKE_SCHEMA")
    snowflake_table: str = Field(..., env="SNOWFLAKE_TABLE")
    slack_webhook_url: str = Field("", env="SLACK_WEBHOOK_URL")

    class Config:
        env_file = ".env"

settings = Settings()