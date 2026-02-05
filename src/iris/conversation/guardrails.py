# src/iris/conversation/guardrails.py
from __future__ import annotations

import os
from typing import Tuple, Optional

import boto3


def _bedrock_runtime_client():
    region = os.getenv("BEDROCK_REGION") or os.getenv("AWS_REGION") or "us-east-1"
    return boto3.client("bedrock-runtime", region_name=region)


def apply_input_guardrail(text: str) -> Tuple[bool, Optional[str], Optional[dict]]:
    """
    Returns:
      (allowed, block_message, raw_response)

    - If BEDROCK_GUARDRAIL_ID is not set -> fail open (allowed=True)
    - If ApplyGuardrail errors -> fail open (allowed=True) but returns raw error via response=None
    - If guardrail intervenes -> allowed=False and block_message from response.outputs[0].text if present
    """
    guardrail_id = os.getenv("BEDROCK_GUARDRAIL_ID", "").strip()
    if not guardrail_id:
        return True, None, None  # guardrails disabled

    version = os.getenv("BEDROCK_GUARDRAIL_VERSION", "DRAFT").strip() or "DRAFT"

    try:
        client = _bedrock_runtime_client()
        resp = client.apply_guardrail(
            guardrailIdentifier=guardrail_id,
            guardrailVersion=version,
            source="INPUT",
            content=[{"text": {"text": text}}],
            outputScope="INTERVENTIONS",
        )

        action = resp.get("action")
        if action == "GUARDRAIL_INTERVENED":
            # Bedrock may return suggested output text in outputs
            outputs = resp.get("outputs") or []
            msg = None
            if outputs and isinstance(outputs[0], dict):
                msg = outputs[0].get("text")
            if not msg:
                msg = "I’m unable to help with that request. Please keep this conversation professional and focused on scheduling."
            return False, msg, resp

        return True, None, resp

    except Exception as e:
        # Fail open by design so your scheduling assistant doesn't break if Bedrock has a hiccup
        print("[guardrail] apply_guardrail failed; failing open:", repr(e))
        return True, None, None
