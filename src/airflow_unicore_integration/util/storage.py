"""Reading single files out of a UNICORE storage, e.g. a job's working directory."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# A plain GET on a UNICORE file resource answers with the file's *metadata*, so the
# content has to be asked for explicitly.
CONTENT_HEADERS = {"Accept": "application/octet-stream"}


def read_storage_file(working_dir: Any, name: str, errors: str = "strict") -> str:
    """
    Return the content of one file of a UNICORE storage as text.

    The historic ``working_dir.stat(name).raw().read()`` does not work any more: pyunicore
    hands out ``httpx.Response.raw``, which recent httpx only exposes as a context manager,
    so ``.read()`` raises AttributeError. Read the resource with one request instead - the
    files read here (job logs, batch submission records, small output files) are far too
    small to be worth streaming.

    :param working_dir: the `pyunicore` storage resource of the job
    :param name: path of the file within that storage
    :param errors: forwarded to `bytes.decode`; ``"replace"`` keeps a single undecodable
        byte from destroying an otherwise readable log, while the default keeps callers
        that must reject binary content able to do so.
    """
    stored_file = working_dir.stat(name)
    response = stored_file.transport.get(
        url=stored_file.resource_url, to_json=False, headers=CONTENT_HEADERS
    )
    return response.content.decode("utf-8", errors)
