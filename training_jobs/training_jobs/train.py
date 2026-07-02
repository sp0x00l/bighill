from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

from training_jobs.manifest import TrainingArtifactManifest
from training_jobs.storage import upload_directory, write_json_bytes


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def main() -> None:
    training_run_id = require_env("TRAINING_RUN_ID")
    recipe_yaml = require_env("TRAINING_RECIPE_YAML")
    model_uri = require_env("TRAINING_MODEL_URI")
    manifest_uri = require_env("TRAINING_ARTIFACT_MANIFEST_URI")
    model_name = require_env("TRAINING_MODEL_NAME")
    model_version = require_env("TRAINING_MODEL_VERSION")
    base_model = require_env("TRAINING_BASE_MODEL")
    recipe_hash = os.environ.get("TRAINING_RECIPE_HASH", "").strip()
    artifact_format = os.environ.get("TRAINING_ARTIFACT_FORMAT", "HF_PEFT_ADAPTER").strip()
    adapter_uri = os.environ.get("TRAINING_ADAPTER_URI", model_uri).strip()
    serving_target = os.environ.get("TRAINING_SERVING_TARGET", "").strip()
    serving_model = os.environ.get("TRAINING_SERVING_MODEL", "").strip()
    serving_load_status = os.environ.get("TRAINING_SERVING_LOAD_STATUS", "LOADED").strip()
    command = require_env("TRAINING_AXOLOTL_COMMAND")

    work_dir = Path(os.environ.get("TRAINING_JOB_WORK_DIR", f"/tmp/training_jobs/{training_run_id}")).resolve()
    output_dir = work_dir / "adapter"
    recipe_path = work_dir / "axolotl.yaml"
    work_dir.mkdir(parents=True, exist_ok=True)
    recipe_path.write_text(recipe_with_local_output(recipe_yaml, output_dir), encoding="utf-8")

    run_training_command(command, recipe_path, output_dir)
    artifact = upload_directory(output_dir, model_uri)
    manifest = TrainingArtifactManifest(
        training_run_id=training_run_id,
        model_uri=model_uri,
        model_name=model_name,
        model_version=model_version,
        base_model=base_model,
        artifact_format=artifact_format,
        artifact_checksum=artifact.checksum,
        artifact_size_bytes=artifact.size_bytes,
        adapter_uri=adapter_uri,
        serving_target=serving_target,
        serving_model=serving_model,
        serving_load_status=serving_load_status,
        recipe_hash=recipe_hash,
    )
    write_json_bytes(manifest_uri, manifest.to_json())


def recipe_with_local_output(recipe_yaml: str, output_dir: Path) -> str:
    lines = recipe_yaml.splitlines()
    replaced = False
    rendered: list[str] = []
    for line in lines:
        if line.startswith("output_dir:"):
            rendered.append(f"output_dir: {output_dir}")
            replaced = True
        else:
            rendered.append(line)
    if not replaced:
        rendered.append(f"output_dir: {output_dir}")
    return "\n".join(rendered) + "\n"


def run_training_command(command: str, recipe_path: Path, output_dir: Path) -> None:
    argv = [
        part.replace("{recipe}", str(recipe_path)).replace("{output_dir}", str(output_dir))
        for part in shlex.split(command)
    ]
    if not any(str(recipe_path) == part for part in argv):
        argv.append(str(recipe_path))
    env = os.environ.copy()
    env["TRAINING_AXOLOTL_RECIPE_PATH"] = str(recipe_path)
    env["TRAINING_OUTPUT_DIR"] = str(output_dir)
    completed = subprocess.run(argv, env=env, cwd=str(recipe_path.parent), check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"training command exited with status {completed.returncode}")
    if not output_dir.is_dir() or not any(output_dir.rglob("*")):
        raise RuntimeError(f"training command did not produce adapter files at {output_dir}")


if __name__ == "__main__":
    main()
