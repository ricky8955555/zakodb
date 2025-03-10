import io
from typing import Generator

from zakodb._io import ZakoDbIO
from zakodb.query import QueryExecutor
from zakodb.types import StrOrBytesPath, ZakoDbEntry, ZakoDbMetadata


class ZakoDb:
    _io: ZakoDbIO
    _metadata: ZakoDbMetadata
    _data_offset: int

    @property
    def metadata(self) -> ZakoDbMetadata:
        return self._metadata

    def __init__(self, io: ZakoDbIO, metadata: ZakoDbMetadata, data_offset: int) -> None:
        self._io = io
        self._metadata = metadata
        self._data_offset = data_offset

    @staticmethod
    def create(path: StrOrBytesPath, metadata: ZakoDbMetadata) -> "ZakoDb":
        io = ZakoDbIO(path)

        io.write_magic()
        io.write_metadata(metadata)

        data_offset = io.underlying.tell()

        return ZakoDb(io, metadata, data_offset)

    @staticmethod
    def load(path: str) -> "ZakoDb":
        io = ZakoDbIO(path)

        io.expect_magic()
        metadata = io.read_metadata()

        data_offset = io.underlying.tell()

        return ZakoDb(io, metadata, data_offset)

    def close(self) -> None:
        self._io.close()

    def append_entry(self, entry: ZakoDbEntry) -> None:
        self._io.underlying.seek(0, io.SEEK_END)
        self._io.write_entry(entry, self._metadata.field_props)

    def iter_entries(self) -> Generator[ZakoDbEntry, None, None]:
        self._io.underlying.seek(self._data_offset, io.SEEK_SET)

        while True:
            try:
                yield self._io.read_entry(self._metadata.field_props)
            except EOFError:
                return

    def find_entry(self, query: QueryExecutor) -> Generator[ZakoDbEntry, None, None]:
        return (entry for entry in self.iter_entries() if query(entry))
