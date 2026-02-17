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
        """Return the socket."""
        return self._sock

    @sock.setter
    def sock(self, value):
        """When modifying the socket, ensure that it is ssl wrapped."""
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

    def parse_entries(self, file_pattern, entries):
        files = []
        for entry in entries:
            fields = entry.split()
            if len(fields) != 4:
                raise ValueError(f"Unhandled entry: '{entry}'")
            if not fields[2].isdigit():  # '<DIR>':
                continue
            file = {
                "last_modified": datetime.strptime(
                    f"{fields[0]} {fields[1]}", "%m-%d-%Y %I:%M%p"
                ),  # date and time when the file was last modified
                "content_length": int(fields[2]),  # size of the file in bytes
                "name": fields[3],  # name of the file
            }
            
            file_date_pattern = "\.\d{8}$"
            
            if not re.match(file_date_pattern, file["name"]):
                file_date = (date.today() - timedelta(days=1)).strftime('%Y%m%d')

            match = re.search(file_pattern, file["name"], re.I)
            if match:
                target = match.groups()[0]
                system_date = match.groups()[1] if len(match.groups()) > 1 else file_date

                if not re.match("^\d{14}$", system_date):
                    system_date += "000000"

                file |= {
                    "target": target.lower(),  # assuming abbreviated name
                    "system_date": datetime.strptime(system_date, "%Y%m%d%H%M%S"),
                }
                files.append(file)

        return pl.DataFrame(files)

    def filter_entries(self, file_pattern, path="", targets=[]):
        entries = self.list_entries(path)
        files = self.parse_entries(file_pattern, entries)
        if targets:
            files = files.filter(pl.col("target").is_in(targets))
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
        

class FTPSWriter:

    def __init__(self, file_obj):
        self.file_obj = file_obj
        self.count = 0

    def __call__(self, payload):
        """
        Count the lines as the file is received and stream to file object
        """
        self.file_obj.write(payload)
        self.count += payload.count(b"\n")

