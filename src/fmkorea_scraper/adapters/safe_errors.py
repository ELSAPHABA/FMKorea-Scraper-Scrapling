def safe_error(exc: Exception, stage: str) -> dict:
    return {"stage": stage, "reason": type(exc).__name__,
        "retryable": False, "safe_message": f"{stage} failed"}
