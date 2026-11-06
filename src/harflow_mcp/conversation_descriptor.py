"""Public descriptive linkage; never sufficient authority to launch or interrupt."""
import re

from .conversation_documents import CONTRACT, ConversationError, identifier, request_uuid


def validate(value):
    if type(value) is not dict or set(value) != {
        "contract", "conversation_id", "transport", "profile_fingerprint",
    } or value["contract"] != CONTRACT:
        raise ConversationError("invalid_conversation_descriptor")
    request_uuid(value["conversation_id"])
    identifier(value["transport"])
    if type(value["profile_fingerprint"]) is not str or re.fullmatch(
        r"[a-f0-9]{64}", value["profile_fingerprint"]
    ) is None:
        raise ConversationError("invalid_conversation_descriptor")
    return value
