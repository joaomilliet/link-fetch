"""Link Fetch: its settings, what the user fills in, in AiAudioHub (Add-ons > Link Fetch).

A `secret` (an API key, a token) is a password field only the user fills in. The hub never shows it to an
assistant. Read it here with need("key"), never from a file or an environment variable, and never put it
in an answer, a log line or an error.
"""
from __future__ import annotations

from .toolkit import SETTINGS, ToolError, VALUES, setting

TITLE = "Link Fetch"

setting("googleApiKey", "secret", "Google Drive API key", "", "Google Cloud Console > APIs & Services > Credentials > API key, with the Google Drive API enabled. Only needed to list shared Drive folders.")
setting("downloadFolder", "text", "Download folder", "~/Downloads/Link Fetch", "Where links are downloaded before they are organised.")


def need(key: str):
    """The value of a setting, or a refusal saying where the user fills it in."""
    value = VALUES.get(key)
    if value is None or value == "":
        label = next((s["label"] for s in SETTINGS if s["key"] == key), key)
        raise ToolError("%s is not set: the user fills it in AiAudioHub, Add-ons > %s" % (label, TITLE),
                        "not_configured", key)
    return value
