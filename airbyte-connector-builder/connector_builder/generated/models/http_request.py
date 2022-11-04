#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

# coding: utf-8

from __future__ import annotations

import re  # noqa: F401
from datetime import date, datetime  # noqa: F401
from typing import Any, Dict, List, Optional  # noqa: F401

from pydantic import AnyUrl, BaseModel, EmailStr, validator  # noqa: F401


class HttpRequest(BaseModel):
    """NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).

    Do not edit the class manually.

    HttpRequest - a model defined in OpenAPI

        url: The url of this HttpRequest.
        parameters: The parameters of this HttpRequest [Optional].
        body: The body of this HttpRequest [Optional].
        headers: The headers of this HttpRequest [Optional].
    """

    url: str
    parameters: Optional[Dict[str, Any]] = None
    body: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, Any]] = None


HttpRequest.update_forward_refs()
