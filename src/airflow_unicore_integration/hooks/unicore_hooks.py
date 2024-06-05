from __future__ import annotations

from  pyunicore import client, credentials

from airflow.hooks.base import BaseHook


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
        self.conn = None

    def get_conn(self, overwrite_base_url: str | None = None, overwrite_credential : credentials.Credential | None = None) -> client.Client:
        """Return a Unicore Client. base_url and credentials may be overwritten."""
        if self.conn is None or overwrite_base_url is not None or overwrite_credential is not None: # if not generated, or overwrite attribute is set crete new
            params = self.get_connection(self.uc_conn_id)
            base_url = params.host
            credential = credentials.UsernamePassword(params.login, params.password)
            if overwrite_base_url is not None:
                base_url = overwrite_base_url
            if overwrite_credential is not None:
                credential = overwrite_credential
            self.conn = client.Client(credential, base_url)

        return self.conn


    def test_connection(self) -> tuple[bool, str]:
        """Test the connection by sending an access_info request"""
        try:
            conn = self.get_conn()
            conn.access_info()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)
