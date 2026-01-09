import re
import ssl
import ftplib
import polars as pl
from datetime import datetime, date, timedelta


class iFTP_TLS(ftplib.FTP_TLS):
    """
    FTPS client with implicit/explicit mode support.
    This file is used by Lambda ONLY when ftps_mode == "implicit".
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_default_certs()

        # Prefer TLS 1.2 for compatibility
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2

        self.context = ctx
        self._mode = "explicit"
        self._disable_epsv = True
        self._conn_args = None

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
            # TLS immediately on connect
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

    def list_entries(self, path):
        entries = []
        self.dir(path, entries.append)
        return entries

    def _parse_dir_line(self, entry: str):
        """
        Handles common Windows/IIS-ish LIST output:
            12-31-2025  10:55PM       12345 filename.ext
            12-31-2025  10:55PM       <DIR> foldername
        """
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

        return {
            "last_modified": last_modified,
            "content_length": int(size_part),
            "name": name_part,
        }

    def parse_entries(self, file_pattern, entries):
        """
        Contract:
          - regex must have >= 1 capture group => group(1) = target
          - optional group(2) => system_date (YYYYMMDD or YYYYMMDDHHMMSS)

        If regex does not provide a date, we use:
          - filename suffix ".YYYYMMDD" if present, else yesterday.
        """
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

            # Normalize to 14 digits
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

    def filter_entries(self, file_pattern, path="", targets=None, list_retries: int = 0):
        targets = targets or []
        entries = self.list_entries(path)
        files = self.parse_entries(file_pattern, entries)

        if files.height == 0:
            return files

        if targets:
            targets_lc = [t.lower() for t in targets]
            files = files.filter(pl.col("target").is_in(targets_lc))

        if files.height == 0:
            return files

        # Keep your original "min day then newest per target" logic
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
