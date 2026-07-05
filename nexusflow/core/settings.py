"""
nexusflow/core/settings.py
Central configuration — loaded once at startup.
"""
from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── App ───────────────────────────────────────────────────────────────────
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    log_level: str = "INFO"

    # ── Database ──────────────────────────────────────────────────────────────
    database_url: str = "sqlite+aiosqlite:///./nexusflow_dev.db"
    audit_db_url: str = "sqlite+aiosqlite:///./nexusflow_audit.db"

    # ── Auth ──────────────────────────────────────────────────────────────────
    secret_key: str = "dev-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60

    # ── IBM Bob ───────────────────────────────────────────────────────────────
    ibm_bob_api_key: str = ""
    ibm_bob_base_url: str = "https://api.ibm.com/bob/v1"

    # ── LLM Routing ───────────────────────────────────────────────────────────
    openrouter_api_key: str = ""
    openrouter_base_url: str = "https://openrouter.ai/api/v1"

    # ── MCP Adapters ──────────────────────────────────────────────────────────
    slack_bot_token: str = ""
    slack_signing_secret: str = ""
    jira_base_url: str = ""
    jira_email: str = ""
    jira_api_token: str = ""
    github_token: str = ""
    salesforce_client_id: str = ""
    salesforce_client_secret: str = ""
    salesforce_instance_url: str = ""
    pagerduty_api_key: str = ""
    datadog_api_key: str = ""
    datadog_app_key: str = ""
    sentry_auth_token: str = ""
    sentry_org_slug: str = ""
    notion_secret: str = ""
    notion_database_id: str = ""
    linear_api_key: str = ""
    confluence_base_url: str = ""
    confluence_email: str = ""
    confluence_api_token: str = ""
    google_calendar_access_token: str = ""
    google_calendar_id: str = ""
    sendgrid_api_key: str = ""
    sendgrid_from_email: str = ""

    # ── Pipeline Config ───────────────────────────────────────────────────────
    policy_file_path: str = "./policies/default.yaml"
    decision_budget_threshold: float = 50_000.0
    pipeline_timeout_seconds: int = 300

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def ibm_bob_enabled(self) -> bool:
        return bool(self.ibm_bob_api_key)


@lru_cache
def get_settings() -> Settings:
    return Settings()
