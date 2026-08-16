from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MODULE_REF = re.compile(r"^[a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9-]*@[0-9]+\.[0-9]+\.[0-9]+$")


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Binding(StrictModel):
    source: str
    optional: bool = False
    default: Any = None

    @model_validator(mode="after")
    def validate_default(self) -> Binding:
        if self.default is not None and not self.optional:
            raise ValueError("binding default requires optional=true")
        return self


class Step(StrictModel):
    id: str
    module: str
    needs: list[str] = Field(default_factory=list)
    when: str | None = None
    bindings: dict[str, str | Binding] = Field(default_factory=dict)

    @field_validator("id")
    @classmethod
    def valid_id(cls, value: str) -> str:
        if not IDENTIFIER.fullmatch(value):
            raise ValueError("step id must be a JMESPath-safe identifier")
        return value

    @field_validator("module")
    @classmethod
    def valid_module(cls, value: str) -> str:
        if not MODULE_REF.fullmatch(value):
            raise ValueError("module must be repository/module@semver")
        return value


class WorkflowMeta(StrictModel):
    id: str
    version: str
    max_parallel: int = Field(default=4, ge=1, le=4)

    @field_validator("id")
    @classmethod
    def valid_id(cls, value: str) -> str:
        if not IDENTIFIER.fullmatch(value):
            raise ValueError("workflow id must be a safe identifier")
        return value


class WorkflowSpec(StrictModel):
    schema_version: Literal["1.0"]
    workflow: WorkflowMeta
    steps: list[Step] = Field(min_length=1)
    outputs: dict[str, str] = Field(default_factory=dict)


class ModuleContract(StrictModel):
    ref: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    schema_version: str = "1.0"


class Deployment(StrictModel):
    base_url: str
    token_env: str | None = None


class RegistrySpec(StrictModel):
    schema_version: Literal["1.0"]
    deployments: dict[str, Deployment]


class RunRequest(StrictModel):
    workflow: dict[str, Any]
    input: Any = None
