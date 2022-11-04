#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

# This file was auto-generated from Airbyte's custom OpenAPI templates
# coding: utf-8

import inspect
from abc import ABC, abstractmethod
from typing import Callable, Dict, List  # noqa: F401

from connector_builder.generated.models.extra_models import TokenModel  # noqa: F401
from connector_builder.generated.models.invalid_input_exception_info import InvalidInputExceptionInfo
from connector_builder.generated.models.known_exception_info import KnownExceptionInfo
from connector_builder.generated.models.stream_read import StreamRead
from connector_builder.generated.models.stream_read_request_body import StreamReadRequestBody
from connector_builder.generated.models.streams_list_read import StreamsListRead
from connector_builder.generated.models.streams_list_request_body import StreamsListRequestBody
from fastapi import APIRouter, Body, Cookie, Depends, Form, Header, Path, Query, Response, Security, status  # noqa: F401


class DefaultApi(ABC):
    @abstractmethod
    async def get_manifest_template(
        self,
    ) -> str:
        """Return a connector manifest template to use as the default value for the yaml editor"""

    @abstractmethod
    async def list_streams(
        self,
        streams_list_request_body: StreamsListRequestBody = Body(None, description=""),
    ) -> StreamsListRead:
        """List all streams present in the connector manifest, along with their specific request URLs"""

    @abstractmethod
    async def read_stream(
        self,
        stream_read_request_body: StreamReadRequestBody = Body(None, description=""),
    ) -> StreamRead:
        """Reads a specific stream in the source. TODO in a later phase - only read a single slice of data."""


def _assert_signature_is_set(method: Callable) -> None:
    """
    APIRouter().add_api_route expects the input method to have a signature. It gets signatures
    by running inspect.signature(method) under the hood.

    In the case that an instance method does not declare "self" as an input parameter (due to developer error
    for example), then the call to inspect.signature() fails.

    This method is sort of a workaround. It tells the developer how to fix the problem if it's detected.
    """
    try:
        inspect.signature(method)
    except ValueError as e:
        # Based on empirical observation, the call to inspect fails with a ValueError
        # with exactly one argument: "invalid method signature"
        if e.args and len(e.args) == 1 and e.args[0] == "invalid method signature":
            # I couldn't figure out how to setattr on a "method" object to populate the signature. For now just kick
            # it back to the developer and tell them to set the "self" variable
            raise Exception(f"Method {method.__name__} in class {type(method.__self__).__name__} must declare the variable 'self'. ")
        else:
            raise


def initialize_router(api: DefaultApi) -> APIRouter:
    router = APIRouter()

    _assert_signature_is_set(api.get_manifest_template)
    router.add_api_route(
        "/v1/manifest_template",
        endpoint=api.get_manifest_template,
        methods=["GET"],
        responses={
            200: {"model": str, "description": "Successful operation"},
        },
        tags=["default"],
        summary="Return a connector manifest template to use as the default value for the yaml editor",
        response_model_by_alias=True,
    )

    _assert_signature_is_set(api.list_streams)
    router.add_api_route(
        "/v1/streams/list",
        endpoint=api.list_streams,
        methods=["POST"],
        responses={
            200: {"model": StreamsListRead, "description": "Successful operation"},
            400: {"model": KnownExceptionInfo, "description": "Exception occurred; see message for details."},
            422: {"model": InvalidInputExceptionInfo, "description": "Input failed validation"},
        },
        tags=["default"],
        summary="List all streams present in the connector manifest, along with their specific request URLs",
        response_model_by_alias=True,
    )

    _assert_signature_is_set(api.read_stream)
    router.add_api_route(
        "/v1/stream/read",
        endpoint=api.read_stream,
        methods=["POST"],
        responses={
            200: {"model": StreamRead, "description": "Successful operation"},
            400: {"model": KnownExceptionInfo, "description": "Exception occurred; see message for details."},
            422: {"model": InvalidInputExceptionInfo, "description": "Input failed validation"},
        },
        tags=["default"],
        summary="Reads a specific stream in the source. TODO in a later phase - only read a single slice of data.",
        response_model_by_alias=True,
    )

    return router
