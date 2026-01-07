import re
import ssl
import ftplib
import polars as pl
from datetime import datetime, date, timedelta


class iFTP_TLS(ftplib.FTP_TLS):
    """
    FTP_TLS subclass that automatically wraps sockets in SSL to support implicit FTPS.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sock = None

    @property
    def sock(self):
        return self._sock

    @sock.setter
    def sock(self, value):
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value

    def make_connection(self, host, port, username, password, log_level=0):
        self.set_debuglevel(log_level)
        self.connect(host=host, port=port)
        self.login(username, password)
        self.prot_p()

    def list_entries(self, path):
        entries = []
        self.dir(path, entries.append)
        return entries

    def _parse_dir_line(self, entry: str):
        """
        Handles common Windows/IIS-ish LIST output like:
            12-31-2025  10:55PM       12345 filename.ext
            12-31-2025  10:55PM       <DIR> foldername

        We only trust first 3 tokens; name is remainder.
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
        Supports all your provided regex patterns.

        Contract (same as your original intent):
          - regex must have >= 1 capture group => group(1) = target
          - optional group(2) => system_date (YYYYMMDD or YYYYMMDDHHMMSS)

        If regex does not provide a date, we use:
          - filename suffix .YYYYMMDD if present, else yesterday
        """
        files = []
        compiled = re.compile(file_pattern, re.I)

        for entry in entries:
            parsed = self._parse_dir_line(entry)
            if not parsed:
                continue

            name = parsed["name"]

            # Default date from suffix ".YYYYMMDD" if present, else yesterday
            m_suffix = re.search(r"\.(\d{8})$", name)
            if m_suffix:
                default_file_date = m_suffix.group(1)
            else:
                default_file_date = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

            match = compiled.search(name)
            if not match:
                continue

            if match.lastindex is None or match.lastindex < 1:
                # No capture group => cannot produce target
                continue

            target = match.group(1)
            system_date_raw = match.group(2) if (match.lastindex and match.lastindex >= 2) else default_file_date

            # Normalize system_date to 14 digits
            if re.fullmatch(r"\d{14}", system_date_raw):
                system_date_14 = system_date_raw
            elif re.fullmatch(r"\d{8}", system_date_raw):
                system_date_14 = system_date_raw + "000000"
            else:
                system_date_14 = default_file_date + "000000"

            files.append({
                **parsed,
                "target": str(target).lower(),
                "system_date": datetime.strptime(system_date_14, "%Y%m%d%H%M%S"),
            })

        return pl.DataFrame(files)

    def filter_entries(self, file_pattern, path="", targets=[]):
        entries = self.list_entries(path)
        files = self.parse_entries(file_pattern, entries)

        if files.height == 0:
            return files

        if targets:
            targets_lc = [t.lower() for t in targets]
            files = files.filter(pl.col("target").is_in(targets_lc))

        if files.height == 0:
            return files

        # Preserve your original logic:
        # - choose the minimum system_date day across all matches
        # - then per target choose the latest system_date + last_modified
        min_system_date = files.select(pl.min("system_date").dt.truncate("1d")).to_series()

        files = files.with_columns(files["system_date"].dt.truncate("1d").alias("sysdate_trunc"))
        files = files.filter(pl.col("sysdate_trunc").is_in(min_system_date))
        files.drop_in_place("sysdate_trunc")

        files = files.groupby("target").agg(
            pl.all()
            .sort_by(
                ["target", "system_date", "last_modified"],
                descending=[False, True, True],
            )
            .first()
        )
        return files
