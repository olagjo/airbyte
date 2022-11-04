#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

# coding: utf-8

from __future__ import annotations

import re  # noqa: F401
from datetime import date, datetime  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from connector_builder.generated.models.stream_read_slices import StreamReadSlices
from pydantic import AnyUrl, BaseModel, EmailStr, validator  # noqa: F401


class StreamRead(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    StreamRead - a model defined in OpenAPI

        logs: The logs of this StreamRead.
        slices: The slices of this StreamRead.
    """

    logs: List[object]
    slices: List[StreamReadSlices]


StreamRead.update_forward_refs()
