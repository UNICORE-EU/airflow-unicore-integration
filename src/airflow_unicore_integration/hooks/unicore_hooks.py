from __future__ import annotations

import logging
from typing import Any

from airflow.providers.common.compat.sdk import BaseHook
from pyunicore import client
from pyunicore import credentials
from wtforms import StringField

logger = logging.getLogger(__name__)


class UnicoreHook(BaseHook):
    """
    Interact with Unicore.

    Creates Unicore Clients from airflow connections.

    :param uc_conn_id: The unicore connection id - default: uc_default
    """

    conn_name_attr = "uc_conn_id"
    default_conn_name = "uc_default"
    conn_type = "unicore"
    hook_name = "Unicore"

    def __init__(self, uc_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.uc_conn_id = uc_conn_id

    @classmethod
    def get_connection_form_fields(cls):
        return {"auth_token": StringField("Auth Token")}

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for UNICORE connection."""
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "login": "Username",
            },
            "placeholder": {"auth_token": "UNICORE auth token"},
        }

    def get_conn(
        self,
        overwrite_base_url: str | None = None,
        overwrite_credential: credentials.Credential | None = None,
    ) -> client.Client:
        """Return a Unicore Client. base_url and credentials may be overwritten."""
        logger.debug(
            f"Gettig connection with id '{self.uc_conn_id}' from secrets backend. Will be modified with user input for UNICORE."
        )
        params = self.get_connection(self.uc_conn_id)
        base_url = params.host
        credential = credentials.UsernamePassword(params.login, params.password)
        auth_token = params.extra_dejson.get("auth_token", None)
        if auth_token is not None:
            credential = credentials.create_credential(token=auth_token)
        if overwrite_base_url is not None:
            base_url = overwrite_base_url
        if overwrite_credential is not None:
            credential = overwrite_credential
        if not base_url:
            raise TypeError()
        conn = client.Client(credential, base_url)
        return conn

    def get_credential(self) -> credentials.Credential:
        """Return the credential part of the connection as a Credential object."""
        params = self.get_connection(self.uc_conn_id)
        credential = credentials.UsernamePassword(params.login, params.password)
        auth_token = params.extra_dejson.get("auth_token", None)
        if auth_token is not None:
            credential = credentials.create_credential(token=auth_token)
        return credential

    def get_base_url(self) -> str:
        """Return the base url of the connection."""
        params = self.get_connection(self.uc_conn_id)
        return params.host

    def test_connection(self) -> tuple[bool, str]:
        """Test the connection by sending an access_info request"""
        conn = self.get_conn()
        conn.access_info()
        return True, "Connection successfully tested"
