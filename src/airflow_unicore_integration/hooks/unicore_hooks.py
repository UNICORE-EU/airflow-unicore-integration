from __future__ import annotations

from  pyunicore import client, credentials

from airflow.hooks.base import BaseHook


class UnicoreHook(BaseHook):
    """
    Interact with Unicore.

    Errors that may occur throughout but should be handled downstream.

    :param uc_conn_id: The unicore connection id
    """

    conn_name_attr = "uc_conn_id"
    default_conn_name = "uc_default"
    conn_type = "unicore"
    hook_name = "Unicore"

    def __init__(self, uc_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.uc_conn_id = uc_conn_id
        self.conn = None

    def get_conn(self) -> client.Client:
        """Return a Unicore Client."""
        if self.conn is None:
            params = self.get_connection(self.uc_conn_id)
            base_url = params.host
            credential = credentials.UsernamePassword(params.login, params.password)
            self.conn = client.Client(credential, base_url)

        return self.conn


    def test_connection(self) -> tuple[bool, str]:
        """Test the FTP connection by calling path with directory."""
        try:
            conn = self.get_conn()
            conn.access_info()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)
