import re
import ssl
import ftplib
import polars as pl
from datetime import datetime, date, timedelta


class FTPSDataHandshakeError(RuntimeError):
    """Raised when the FTPS data-channel TLS handshake fails (often session reuse/resumption related)."""


class iFTP_TLS(ftplib.FTP_TLS):
    """
    Robust FTPS client supporting:
      - explicit FTPS (AUTH TLS)
      - implicit FTPS (TLS-on-connect)
      - strict TLS session reuse for data connections (fixes 522)
      - TLS 1.2 only + no session tickets (reduces EOF/resumption issues)
      - retry-once by reconnecting the control session if data handshake EOF occurs
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_default_certs()

        # Force TLS 1.2 only (many FTPS servers break reuse with TLS 1.3)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2

        # Prefer session-id style reuse
        if hasattr(ssl, "OP_NO_TICKET"):
            ctx.options |= ssl.OP_NO_TICKET

        self.context = ctx

        self._mode = "explicit"        # explicit | implicit
        self._disable_epsv = True
        self._conn_args = None         # store last make_connection params for reconnect

    def set_mode(self, mode: str):
        mode = (mode or "explicit").lower().strip()
        if mode not in ("explicit", "implicit"):
            raise ValueError("mode must be 'explicit' or 'implicit'")
        self._mode = mode

    def set_disable_epsv(self, disable_epsv: bool):
        self._disable_epsv = bool(disable_epsv)

    def connect(self, host="", port=0, timeout=-999, source_address=None):
        super().connect(host=host, port=port, timeout=timeout, source_address=source_address)

        if self._mode == "implicit":
            self.sock = self.context.wrap_socket(self.sock, server_hostname=self.host)
            self.file = self.sock.makefile("r", encoding=self.encoding)

        return self.welcome

    def make_connection(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        log_level: int = 0,
        mode: str = "explicit",
        disable_epsv: bool = True,
    ):
        self.set_debuglevel(log_level)
        self.set_mode(mode)
        self.set_disable_epsv(disable_epsv)

        # Save for reconnect
        self._conn_args = dict(
            host=host,
            port=port,
            username=username,
            password=password,
            log_level=log_level,
            mode=mode,
            disable_epsv=disable_epsv,
        )

        self.set_pasv(True)
        self.use_epsv = not self._disable_epsv

        self.connect(host=host, port=port)

        if self._mode == "explicit":
            self.auth()

        self.login(user=username, passwd=password)
        self.prot_p()

    def _safe_quit_close(self):
        try:
            self.quit()
        except Exception:
            try:
                self.close()
            except Exception:
                pass

    def reconnect(self):
        """
        Fully tears down and recreates the FTPS connection using the last make_connection args.
        """
        if not self._conn_args:
            raise RuntimeError("Cannot reconnect: make_connection() has not been called yet.")

        self._safe_quit_close()

        # Re-init underlying ftplib state by calling __init__ again? No—create a new socket via connect/login.
        # ftplib objects can be reused after close, as long as connect/login occurs again.
        self.make_connection(**self._conn_args)

    # ---- KEY: strict TLS session reuse for data connections ----
    def ntransfercmd(self, cmd, rest=None):
        conn, size = super().ntransfercmd(cmd, rest=rest)

        if self._prot_p:
            session = getattr(self.sock, "session", None)
            if session is None:
                try:
                    conn.close()
                except Exception:
                    pass
                raise FTPSDataHandshakeError(
                    "FTPS server requires TLS session reuse, but control connection has no cached SSL session."
                )

            try:
                conn = self.context.wrap_socket(conn, server_hostname=self.host, session=session)
            except ssl.SSLError as e:
                try:
                    conn.close()
                except Exception:
                    pass
                raise FTPSDataHandshakeError(
                    f"FTPS data-channel TLS handshake failed during session reuse: {e!r}"
                ) from e

        return conn, size
    # -----------------------------------------------------------

    def list_entries(self, path):
        entries = []
        self.dir(path, entries.append)
        return entries

    def list_entries_with_retry(self, path: str, retries: int = 1):
        """
        Some servers intermittently fail the data-channel session-resume handshake (EOF).
        Recovery: reconnect control session and retry once.
        """
        attempt = 0
        while True:
            try:
                return self.list_entries(path)
            except FTPSDataHandshakeError as e:
                attempt += 1
                if attempt > retries:
                    raise
                # Reconnect control session and retry
                print(f"Data-channel handshake failed; reconnecting control session and retrying LIST (attempt {attempt})")
                self.reconnect()

    def _parse_dir_line(self, entry: str):
        fields = entry.split()
        if len(fields) < 4:
            return None

        dt_part = f"{fields[0]} {fields[1]}"
        size_part = fields[2]
        name_part = " ".join(fields[3:]).strip()

        if size_part.upper() == "<DIR>":
            return None
        if not size_part.isdigit():
            return None

        try:
            last_modified = datetime.strptime(dt_part, "%m-%d-%Y %I:%M%p")
        except Exception:
            return None

        return {"last_modified": last_modified, "content_length": int(size_part), "name": name_part}

    def parse_entries(self, file_pattern, entries):
        files = []
        compiled = re.compile(file_pattern, re.I)

        for entry in entries:
            parsed = self._parse_dir_line(entry)
            if not parsed:
                continue

            name = parsed["name"]

            m_suffix = re.search(r"\.(\d{8})$", name)
            default_file_date = m_suffix.group(1) if m_suffix else (date.today() - timedelta(days=1)).strftime("%Y%m%d")

            match = compiled.search(name)
            if not match:
                continue
            if match.lastindex is None or match.lastindex < 1:
                continue

            target = match.group(1)
            system_date_raw = match.group(2) if (match.lastindex and match.lastindex >= 2) else default_file_date

            if re.fullmatch(r"\d{14}", system_date_raw):
                system_date_14 = system_date_raw
            elif re.fullmatch(r"\d{8}", system_date_raw):
                system_date_14 = system_date_raw + "000000"
            else:
                system_date_14 = default_file_date + "000000"

            files.append(
                {
                    **parsed,
                    "target": str(target).lower(),
                    "system_date": datetime.strptime(system_date_14, "%Y%m%d%H%M%S"),
                }
            )

        return pl.DataFrame(files)

    def filter_entries(self, file_pattern, path="", targets=[], list_retries: int = 1):
        # Use retrying LIST (reconnect once if EOF during data handshake)
        entries = self.list_entries_with_retry(path, retries=list_retries)

        files = self.parse_entries(file_pattern, entries)
        if files.height == 0:
            return files

        if targets:
            targets_lc = [t.lower() for t in targets]
            files = files.filter(pl.col("target").is_in(targets_lc))

        if files.height == 0:
            return files

        min_system_date = files.select(pl.min("system_date").dt.truncate("1d")).to_series()

        files = files.with_columns(files["system_date"].dt.truncate("1d").alias("sysdate_trunc"))
        files = files.filter(pl.col("sysdate_trunc").is_in(min_system_date))
        files.drop_in_place("sysdate_trunc")

        files = files.groupby("target").agg(
            pl.all()
            .sort_by(["target", "system_date", "last_modified"], descending=[False, True, True])
            .first()
        )
        return files
