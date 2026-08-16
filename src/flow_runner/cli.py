from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from .config import Settings
from .services import build_services
from .workflow import load_document, validate_workflow


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="flow-runner")
    result.add_argument("--registry", help="private deployment registry YAML/JSON")
    commands = result.add_subparsers(dest="command", required=True)
    validate_parser = commands.add_parser("validate", help="validate workflow syntax and DAG")
    validate_parser.add_argument("workflow")
    plan_parser = commands.add_parser("plan", help="validate deployments and module contracts")
    plan_parser.add_argument("workflow")
    run_parser = commands.add_parser("run", help="execute a workflow")
    run_parser.add_argument("workflow")
    run_parser.add_argument("--input", default="{}", help="inline JSON workflow input")
    status_parser = commands.add_parser("status", help="read persisted run metadata")
    status_parser.add_argument("run_id")
    return result


async def _remote_command(args: argparse.Namespace) -> dict[str, Any]:
    settings = Settings.from_env(registry_path=args.registry)
    services = await build_services(settings)
    try:
        if args.command == "plan":
            return await services.runner.plan(load_document(args.workflow))
        if args.command == "run":
            return await services.runner.run(load_document(args.workflow), json.loads(args.input))
        if args.command == "status":
            value = await services.store.get_run(args.run_id)
            if value is None:
                raise RuntimeError("run not found")
            return value
        raise RuntimeError(f"unsupported command: {args.command}")
    finally:
        await services.close()


def main() -> None:
    args = parser().parse_args()
    if args.command == "validate":
        spec = validate_workflow(load_document(args.workflow))
        print(json.dumps({"valid": True, "workflow": spec.workflow.model_dump()}, indent=2))
        return
    print(json.dumps(asyncio.run(_remote_command(args)), indent=2))


if __name__ == "__main__":
    main()

