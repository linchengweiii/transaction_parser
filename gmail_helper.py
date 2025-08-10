from __future__ import annotations

from pathlib import Path
from typing import Iterator, List, Optional

from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

import base64


class GmailHelper:
    """
    Gmail API helper for:
      - authenticating via OAuth
      - searching messages
      - downloading PDF attachments (skips S/MIME signatures)
    """

    def __init__(
        self,
        credentials_path: Path | str,
        token_path: Path | str,
    ):
        self.credentials_path = Path(credentials_path)
        self.token_path = Path(token_path)
        self.scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
        self.service = self._build_service()  # Build on init

    def _build_service(self):
        """Authenticate and build the Gmail API service."""
        creds: Optional[Credentials] = None

        if self.token_path.exists():
            creds = Credentials.from_authorized_user_file(
                str(self.token_path),
                self.scopes,
            )

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(self.credentials_path),
                    self.scopes
                )
                creds = flow.run_local_server(port=0)
            with open(self.token_path, "w") as token:
                token.write(creds.to_json())

        return build("gmail", "v1", credentials=creds)

    def search_messages(
        self,
        user_id: str,
        query: str,
        max_results: int = 50,
    ) -> List[str]:
        """
        Search for message IDs that match a Gmail search query.
        Handles pagination to satisfy max_results across pages.
        """
        ids: List[str] = []
        page_token: Optional[str] = None

        while len(ids) < max_results:
            resp = (
                self.service.users()
                .messages()
                .list(
                    userId=user_id,
                    q=query,
                    maxResults=min(100, max_results - len(ids)),
                    pageToken=page_token,
                )
                .execute()
            )
            ids.extend([m["id"] for m in resp.get("messages", [])])
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        return ids[:max_results]

    def download_attachments(
        self,
        user_id: str,
        msg_id: str,
        save_dir: Path,
        filename_contains: Optional[str] = None,
    ) -> List[Path]:
        """
        Download real PDF attachments (skip S/MIME signatures like smime.p7s).
        """
        save_dir = Path(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)

        msg = self.service.users().messages().get(
            userId=user_id,
            id=msg_id,
            format="full"
        ).execute()
        payload = msg.get("payload", {}) or {}

        downloaded: List[Path] = []
        for part in self._walk_parts(payload):
            mime = (part.get("mimeType") or "").lower()
            filename = part.get("filename") or ""

            # Skip containers and signatures
            if mime.startswith("multipart/"):
                continue
            if filename.lower().endswith(".p7s") or "pkcs7-signature" in mime:
                continue

            # Keep only PDFs
            is_pdf = mime == "application/pdf" or filename.lower().endswith(".pdf")
            if not is_pdf:
                continue

            if filename_contains and filename_contains.lower() not in filename.lower():
                continue

            body = part.get("body", {}) or {}
            data_bytes = None

            if "attachmentId" in body:
                att_id = body["attachmentId"]
                att = (
                    self.service.users()
                    .messages()
                    .attachments()
                    .get(userId=user_id, messageId=msg_id, id=att_id)
                    .execute()
                )
                data_bytes = base64.urlsafe_b64decode(att["data"].encode("utf-8"))
            elif "data" in body:
                data_bytes = base64.urlsafe_b64decode(body["data"].encode("utf-8"))
            else:
                continue

            # Ensure a filename
            if not filename:
                filename = "attachment.pdf"

            # Avoid name collisions
            out_path = self._unique_path(save_dir / filename)

            with open(out_path, "wb") as f:
                f.write(data_bytes)

            downloaded.append(out_path)

        return downloaded

    @staticmethod
    def _walk_parts(part: dict) -> Iterator[dict]:
        """Yield this part and all descendants (Gmail MIME trees can nest)."""
        yield part
        for child in (part.get("parts", []) or []):
            yield from GmailHelper._walk_parts(child)

    @staticmethod
    def _unique_path(path: Path) -> Path:
        """Return a non-conflicting path by suffixing _1, _2, ... if needed."""
        if not path.exists():
            return path
        stem, suf = path.stem, path.suffix or ".pdf"
        k = 1
        while True:
            candidate = path.with_name(f"{stem}_{k}{suf}")
            if not candidate.exists():
                return candidate
            k += 1
