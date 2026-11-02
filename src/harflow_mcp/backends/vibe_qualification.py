"""Feature receipts for measured Vibe ACP permissions.

Seeds apply only to the measured release/platform. ACP additionally verifies its
own executable identity and initialization version before sending any prompt.
"""
from __future__ import annotations

import re

VERSION = "3.0.3"
FEATURES = ("caller_approval_tools", "caller_write_tools", "allowed_commands", "acp_transport", "model_selection")


def parse_version(observed):
    found = re.fullmatch(r"vibe (\d+\.\d+\.\d+)", observed.strip()) if isinstance(observed, str) else None
    return found.group(1) if found else None


def feature_specs():
    from .qualification import FeatureSpec
    return {feature: FeatureSpec(feature, "vibe-acp-permissions-v1",
             seeds={VERSION: {feature: True}}, platforms=("darwin",), seed_platforms=("darwin",))
            for feature in FEATURES}


def qualify(backend, feature):
    from .qualification import require
    return require(backend, feature).identity.version
