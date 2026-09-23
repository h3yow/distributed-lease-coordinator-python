"""错误类型定义：所有失败都带有可区分的机器可读错误码。"""

from __future__ import annotations


class LeaseError(Exception):
    """租约领域错误基类。"""

    code: str = "lease_error"
    http_status: int = 400

    def __init__(self, message: str, *, resource_id: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.resource_id = resource_id

    def to_dict(self) -> dict[str, str | None]:
        return {
            "error": self.code,
            "message": self.message,
            "resource_id": self.resource_id,
        }


class ResourceHeld(LeaseError):
    """资源已被其他有效持有者占用。"""

    code = "resource_held"
    http_status = 409


class InvalidToken(LeaseError):
    """持有凭证不匹配。"""

    code = "invalid_token"
    http_status = 403


class LeaseExpired(LeaseError):
    """租约已过期（操作对象未持有有效租约）。"""

    code = "lease_expired"
    http_status = 410


class LeaseReleased(LeaseError):
    """租约已释放。"""

    code = "lease_released"
    http_status = 410


class LeaseNotFound(LeaseError):
    """租约或资源不存在。"""

    code = "lease_not_found"
    http_status = 404


class ConflictRetryLimit(LeaseError):
    """乐观并发冲突重试次数耗尽。"""

    code = "conflict_retry_limit"
    http_status = 503
