#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from io import StringIO
import json
import logging
from typing import Any, Dict, Iterable, Mapping, Optional, TextIO, Tuple, Union

import polars as pl

from airbyte_cdk.sources.file_based.config.file_based_stream_config import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.exceptions import FileBasedSourceError, RecordParseError
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader, FileReadMode
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_helpers import PYTHON_TYPE_MAPPING, SchemaType, merge_schemas
from orjson import orjson


class JsonlParser(FileTypeParser):

    MAX_BYTES_PER_FILE_FOR_SCHEMA_INFERENCE = 1_000_000
    ENCODING = "utf8"

    def check_config(self, config: FileBasedStreamConfig) -> Tuple[bool, Optional[str]]:
        """
        JsonlParser does not require config checks, implicit pydantic validation is enough.
        """
        return True, None

    async def infer_schema(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
    ) -> SchemaType:
        """
        Infers the schema for the file by inferring the schema for each line, and merging
        it with the previously-inferred schema.
        """
        inferred_schema: Mapping[str, Any] = {}

        for entry in self._parse_jsonl_entries(file, stream_reader, logger, read_limit=True):
            line_schema = self._infer_schema_for_record(entry)
            inferred_schema = merge_schemas(inferred_schema, line_schema)

        return inferred_schema

    def parse_records(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        discovered_schema: Optional[Mapping[str, SchemaType]],
    ) -> Iterable[Dict[str, Any]]:
        """
        This code supports parsing json objects over multiple lines even though this does not align with the JSONL format. This is for
        backward compatibility reasons i.e. the previous source-s3 parser did support this. The drawback is:
        * performance as the way we support json over multiple lines is very brute forced
        * given that we don't have `newlines_in_values` config to scope the possible inputs, we might parse the whole file before knowing if
          the input is improperly formatted or if the json is over multiple lines

        The goal is to run the V4 of source-s3 in production, track the warning log emitted when there are multiline json objects and
        deprecate this feature if it's not a valid use case.
        """
        yield from self._parse_jsonl_entries(file, stream_reader, logger)

    @classmethod
    def _infer_schema_for_record(cls, record: Dict[str, Any]) -> Dict[str, Any]:
        record_schema = {}
        for key, value in record.items():
            if value is None:
                record_schema[key] = {"type": "null"}
            else:
                record_schema[key] = {"type": PYTHON_TYPE_MAPPING[type(value)]}

        return record_schema

    @property
    def file_read_mode(self) -> FileReadMode:
        return FileReadMode.READ

    def _parse_jsonl_entries(
        self,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        read_limit: bool = False,
    ) -> Iterable[Dict[str, Any]]:
        """Parse records and emit as iterable of dictionaries."""
        with stream_reader.open_file(file, self.file_read_mode, self.ENCODING, logger) as fp:
            read_bytes = 0

            had_json_parsing_error = False
            has_warned_for_multiline_json_object = False
            yielded_at_least_once = False

            accumulator = None
            for line in fp:
                if not accumulator:
                    accumulator = self._instantiate_accumulator(line)
                read_bytes += len(line)
                accumulator += line  # type: ignore [operator]  # In reality, it's either bytes or string and we add the same type
                try:
                    record = orjson.loads(accumulator)
                    if had_json_parsing_error and not has_warned_for_multiline_json_object:
                        logger.warning(f"File at {file.uri} is using multiline JSON. Performance could be greatly reduced")
                        has_warned_for_multiline_json_object = True

                    yield record
                    yielded_at_least_once = True
                    accumulator = self._instantiate_accumulator(line)
                except orjson.JSONDecodeError:
                    had_json_parsing_error = True

                if read_limit and yielded_at_least_once and read_bytes >= self.MAX_BYTES_PER_FILE_FOR_SCHEMA_INFERENCE:
                    logger.warning(
                        f"Exceeded the maximum number of bytes per file for schema inference ({self.MAX_BYTES_PER_FILE_FOR_SCHEMA_INFERENCE}). "
                        f"Inferring schema from an incomplete set of records."
                    )
                    break

            if had_json_parsing_error and not yielded_at_least_once:
                raise RecordParseError(FileBasedSourceError.ERROR_PARSING_RECORD, filename=file.uri, lineno=line)

    @staticmethod
    def _instantiate_accumulator(line: Union[bytes, str]) -> Union[bytes, str]:
        if isinstance(line, bytes):
            return bytes("", json.detect_encoding(line))
        elif isinstance(line, str):
            return ""

    def parse_records_as_dataframes(
        self,
        config: FileBasedStreamConfig,
        file: RemoteFile,
        stream_reader: AbstractFileBasedStreamReader,
        logger: logging.Logger,
        discovered_schema: Optional[Mapping[str, SchemaType]],
    ) -> Iterable[pl.DataFrame | pl.LazyFrame]:
        """Parse records and emit as iterable of data frames.

        Currently this only returns an iterator containing a single data frame. This may
        be updated in the future to return an iterator with multiple DataFrames.
        """
        with stream_reader.open_file(
            file=file,
            mode=self.file_read_mode,
            encoding=None,
            logger=logger,
        ) as s3_file:

            def batch_read_lines(
                file_obj: TextIO,
                batch_size: int = 500_000,
            ) -> Iterable[StringIO]:
                buffer = StringIO()  # Initialize StringIO buffer
                count = 0

                for line in file_obj:
                    buffer.write(line)  # Write each line directly to the StringIO buffer
                    count += 1
                    if count >= batch_size:
                        buffer.seek(0)  # Move the pointer to the start of the buffer for reading
                        yield buffer  # Yield the buffer
                        buffer = StringIO()  # Reset the buffer for the next batch
                        count = 0

                if count > 0:  # Yield any remaining lines in the buffer
                    buffer.seek(0)
                    yield buffer
                # buffer: list[str] = []
                # for line in s3_file:
                #     buffer.append(line)
                #     if len(buffer) >= batch_size:
                #         # Yield the batch as a single string
                #         yield StringIO("\n".join(buffer))
                #         buffer = []

            for batch in batch_read_lines(s3_file):
                df: pl.DataFrame = pl.read_ndjson(
                    source=batch,
                    # schema=schema,  # TODO: Add detected schema
                    infer_schema_length=10,
                    # low_memory=True,
                )
                if df.height == 0:  # No more rows to read
                    break

                transformed_df = df.with_columns(
                    pl.lit(file.uri).alias("_ab_source_file_url"),
                    pl.lit(file.last_modified).alias("_ab_source_file_last_modified")
                )
                yield transformed_df

        # # The incoming URI is actually a relative path. We need the absolute ref, for
        # # instance: including the 's3://' protocol, bucket name, etc.
        # fully_qualified_uri = stream_reader.get_fully_qualified_uri(file.uri.split("#")[0])
        # storage_options = stream_reader.polars_storage_options
        # logger.info("Using bulk processing mode to read JSONL file: %s", fully_qualified_uri)

        # lazyframe: pl.LazyFrame = pl.scan_ndjson(
        #     fully_qualified_uri,
        #     storage_options=storage_options,
        #     row_index_name="_ab_record_index",
        #     infer_schema_length=10_000,
        # ).with_columns(
        #     pl.lit(file.uri).alias("_ab_source_file_url"),
        #     pl.lit(file.last_modified).alias("_ab_source_file_last_modified")
        # )

        # def slice_generator(
        #     lazyframe: pl.LazyFrame,
        #     batch_size: int = 50_000,
        # ) -> Iterable[pl.DataFrame]:
        #     offset = 0
        #     while True:
        #         slice = lazyframe.slice(offset=offset, length=batch_size).collect(streaming=True)
        #         height = slice.height
        #         if height == 0:
        #             break
        #         yield slice

        # yield from slice_generator(lazyframe)
