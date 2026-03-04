from __future__ import annotations

from dataclasses import dataclass

try:
    import magic
except ImportError:  # pragma: no cover - environment dependent
    magic = None

from apps.leads.models import LeadAttachment


ALLOWED_ATTACHMENT_MIME_TO_KIND = {
    "audio/mpeg": LeadAttachment.Kind.AUDIO,
    "audio/wav": LeadAttachment.Kind.AUDIO,
    "audio/x-wav": LeadAttachment.Kind.AUDIO,
    "audio/ogg": LeadAttachment.Kind.AUDIO,
    "audio/mp4": LeadAttachment.Kind.AUDIO,
    "audio/x-m4a": LeadAttachment.Kind.AUDIO,
    "image/jpeg": LeadAttachment.Kind.IMAGE,
    "image/png": LeadAttachment.Kind.IMAGE,
    "image/webp": LeadAttachment.Kind.IMAGE,
    "image/gif": LeadAttachment.Kind.IMAGE,
}


@dataclass
class AttachmentValidationError(Exception):
    message: str
    field: str = "file"

    def __str__(self) -> str:
        return self.message


def _read_file_head(uploaded_file, size: int = 8192) -> bytes:
    current_pos = uploaded_file.tell() if hasattr(uploaded_file, "tell") else None
    try:
        if hasattr(uploaded_file, "seek"):
            uploaded_file.seek(0)
        head = uploaded_file.read(size)
    finally:
        if current_pos is not None and hasattr(uploaded_file, "seek"):
            uploaded_file.seek(current_pos)
    return head or b""


def validate_uploaded_attachment(uploaded_file, *, requested_kind: str | None = None) -> tuple[str, str]:
    if magic is None:
        raise AttachmentValidationError("python-magic is not installed in the current environment")

    head = _read_file_head(uploaded_file)
    detected_mime = (magic.from_buffer(head, mime=True) or "").lower().strip()
    resolved_kind = ALLOWED_ATTACHMENT_MIME_TO_KIND.get(detected_mime)
    if resolved_kind is None:
        raise AttachmentValidationError("Only audio and image files are allowed")

    if requested_kind and requested_kind != resolved_kind:
        raise AttachmentValidationError(
            f"Detected file type is {resolved_kind}, but requested kind is {requested_kind}",
            field="kind",
        )

    return resolved_kind, detected_mime
